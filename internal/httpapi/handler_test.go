package httpapi

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	irodsfs "github.com/cyverse/go-irodsclient/fs"
	irodscommon "github.com/cyverse/go-irodsclient/irods/common"
	irodstypes "github.com/cyverse/go-irodsclient/irods/types"
	metadataext "github.com/michael-conway/go-irodsclient-extensions/metadata"
	s3adminext "github.com/michael-conway/go-irodsclient-extensions/s3admin"
	usersandgroupsext "github.com/michael-conway/go-irodsclient-extensions/usersandgroups"
	usersyncext "github.com/michael-conway/go-irodsclient-extensions/usersync"
	"github.com/michael-conway/irods-go-rest/internal/auth"
	"github.com/michael-conway/irods-go-rest/internal/config"
	"github.com/michael-conway/irods-go-rest/internal/domain"
	"github.com/michael-conway/irods-go-rest/internal/irods"
	"github.com/michael-conway/irods-go-rest/internal/restservice"
)

func TestHealthz(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestCORSPreflightAllowsConfiguredOrigin(t *testing.T) {
	handler, _ := testHandlerWithConfig(t, func(cfg *config.RestConfig) {
		cfg.CORSAllowedOrigins = []string{"http://localhost:8081/"}
	})

	req := httptest.NewRequest(http.MethodOptions, "/api/v1/path?irods_path=/tempZone/home/test1", nil)
	req.Header.Set("Origin", "http://localhost:8081")
	req.Header.Set("Access-Control-Request-Method", http.MethodGet)
	req.Header.Set("Access-Control-Request-Headers", "authorization, content-type, <script>alert(1)</script>")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "http://localhost:8081" {
		t.Fatalf("expected allowed origin header, got %q", got)
	}
	if got := rec.Header().Get("Access-Control-Allow-Headers"); got != defaultCORSAllowedHeaders {
		t.Fatalf("expected default allowed headers, got %q", got)
	}
	if got := rec.Header().Get("Access-Control-Allow-Methods"); !strings.Contains(got, http.MethodGet) || !strings.Contains(got, http.MethodOptions) {
		t.Fatalf("expected CORS methods to include GET and OPTIONS, got %q", got)
	}
}

func TestCORSPreflightIgnoresInvalidConfiguredOrigin(t *testing.T) {
	handler, _ := testHandlerWithConfig(t, func(cfg *config.RestConfig) {
		cfg.CORSAllowedOrigins = []string{"http://good.example", "http://localhost:8081/<script>"}
	})

	req := httptest.NewRequest(http.MethodOptions, "/api/v1/path?irods_path=/tempZone/home/test1", nil)
	req.Header.Set("Origin", "http://localhost:8081")
	req.Header.Set("Access-Control-Request-Method", http.MethodGet)
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("expected no allowed origin header, got %q", got)
	}
}

func TestCORSPreflightRejectsUnconfiguredOrigin(t *testing.T) {
	handler, _ := testHandlerWithConfig(t, func(cfg *config.RestConfig) {
		cfg.CORSAllowedOrigins = []string{"http://localhost:8081"}
	})

	req := httptest.NewRequest(http.MethodOptions, "/api/v1/path?irods_path=/tempZone/home/test1", nil)
	req.Header.Set("Origin", "http://example.invalid")
	req.Header.Set("Access-Control-Request-Method", http.MethodGet)
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("expected no allowed origin header, got %q", got)
	}
}

func TestOpenAPISpec(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/openapi.yaml", nil)
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	if got := rec.Header().Get("Content-Type"); !strings.Contains(got, "application/yaml") {
		t.Fatalf("expected yaml content type, got %q", got)
	}

	if body := rec.Body.String(); !containsAll(body, "openapi: 3.0.3", "title: iRODS REST API") {
		t.Fatalf("unexpected response body: %q", body)
	}
}

func TestOpenAPISpecUsesRequestHost(t *testing.T) {
	handler, _ := testHandlerWithConfig(t, func(cfg *config.RestConfig) {
		cfg.PublicURL = ""
		cfg.TrustForwardedHeaders = false
	})

	req := httptest.NewRequest(http.MethodGet, "/openapi.yaml", nil)
	req.Host = "rest.example.org:18082"
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "url: http://rest.example.org:18082") {
		t.Fatalf("expected request host in openapi server url, got %q", body)
	}
}

func TestOpenAPISpecIgnoresForwardedHostAndProtoByDefault(t *testing.T) {
	handler, _ := testHandlerWithConfig(t, func(cfg *config.RestConfig) {
		cfg.PublicURL = ""
		cfg.TrustForwardedHeaders = false
	})

	req := httptest.NewRequest(http.MethodGet, "/openapi.yaml", nil)
	req.Host = "internal:8080"
	req.Header.Set("X-Forwarded-Host", "rest.example.org")
	req.Header.Set("X-Forwarded-Proto", "https")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "url: http://internal:8080") {
		t.Fatalf("expected request host when forwarded headers are untrusted, got %q", body)
	}
}

func TestOpenAPISpecUsesForwardedHostAndProtoWhenTrusted(t *testing.T) {
	handler, _ := testHandlerWithConfig(t, func(cfg *config.RestConfig) {
		cfg.PublicURL = ""
		cfg.TrustForwardedHeaders = true
	})

	req := httptest.NewRequest(http.MethodGet, "/openapi.yaml", nil)
	req.Host = "internal:8080"
	req.Header.Set("X-Forwarded-Host", "rest.example.org")
	req.Header.Set("X-Forwarded-Proto", "https")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "url: https://rest.example.org") {
		t.Fatalf("expected trusted forwarded host and proto in openapi server url, got %q", body)
	}
}

func TestOpenAPISpecUsesConfiguredPublicURLOverRequestHeaders(t *testing.T) {
	handler, _ := testHandlerWithConfig(t, func(cfg *config.RestConfig) {
		cfg.PublicURL = "https://api.example.org"
		cfg.TrustForwardedHeaders = true
	})

	req := httptest.NewRequest(http.MethodGet, "/openapi.yaml", nil)
	req.Host = "internal:8080"
	req.Header.Set("X-Forwarded-Host", "rest.example.org")
	req.Header.Set("X-Forwarded-Proto", "https")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "url: https://api.example.org") {
		t.Fatalf("expected configured public url to take precedence, got %q", body)
	}
}

func TestSwaggerUI(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/swagger", nil)
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	if got := rec.Header().Get("Content-Type"); !strings.Contains(got, "text/html") {
		t.Fatalf("expected html content type, got %q", got)
	}

	if body := rec.Body.String(); !containsAll(body, "SwaggerUIBundle", "/openapi.yaml") {
		t.Fatalf("unexpected response body: %q", body)
	}
}

func TestWebLoginRedirect(t *testing.T) {
	handler, _ := testHandlerWithConfig(t, func(cfg *config.RestConfig) {
		cfg.WebEnabled = true
	})

	req := httptest.NewRequest(http.MethodGet, "/web/login", nil)
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusFound {
		t.Fatalf("expected 302, got %d", rec.Code)
	}
}

func TestWebHomeDisplaysBearerToken(t *testing.T) {
	handler, _ := testHandlerWithConfig(t, func(cfg *config.RestConfig) {
		cfg.WebEnabled = true
	})

	session, err := handler.webSession.Create(auth.Principal{
		Subject:  "user-123",
		Username: "alice",
		Active:   true,
	}, auth.Token{
		AccessToken: "token123",
		TokenType:   "Bearer",
		ExpiresIn:   300,
	})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/web/", nil)
	req.AddCookie(&http.Cookie{Name: webSessionCookieName, Value: session.ID})
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	body := rec.Body.String()
	if !containsAll(body, "Bearer Token", "token123", "Copy token") {
		t.Fatalf("unexpected response body: %q", body)
	}
}

func TestWebCallbackCreatesSession(t *testing.T) {
	handler, _ := testHandlerWithConfig(t, func(cfg *config.RestConfig) {
		cfg.WebEnabled = true
	})

	req := httptest.NewRequest(http.MethodGet, "/web/callback?code=code123&state=state123", nil)
	req.AddCookie(&http.Cookie{Name: authStateCookieName, Value: "state123"})
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusFound {
		t.Fatalf("expected 302, got %d", rec.Code)
	}
}

func TestWebCallbackSurfacesOAuthError(t *testing.T) {
	handler, _ := testHandlerWithConfig(t, func(cfg *config.RestConfig) {
		cfg.WebEnabled = true
	})

	req := httptest.NewRequest(http.MethodGet, "/web/callback?error=access_denied&error_description=user+canceled&state=state123", nil)
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("expected 502, got %d", rec.Code)
	}

	if got := rec.Body.String(); got == "" || !containsAll(got, `"code":"auth_failed"`, `access_denied: user canceled`) {
		t.Fatalf("unexpected response body: %q", got)
	}
}

func TestWebRoutesDisabledWhenConfigured(t *testing.T) {
	handler, _ := testHandlerWithConfig(t, func(cfg *config.RestConfig) {
		cfg.WebEnabled = false
	})

	req := httptest.NewRequest(http.MethodGet, "/web/login", nil)
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestAPIRequiresBearerToken(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/path?irods_path=/tempZone/home/test1/file.txt", nil)
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestMutatingEndpointsRequireAuthentication(t *testing.T) {
	handler := testHandler(t)

	tests := []struct {
		name        string
		method      string
		target      string
		body        string
		contentType string
	}{
		{
			name:        "create user",
			method:      http.MethodPost,
			target:      "/api/v1/user",
			body:        `{"name":"charlie","type":"rodsuser"}`,
			contentType: "application/json",
		},
		{
			name:        "update user",
			method:      http.MethodPut,
			target:      "/api/v1/user/bob",
			body:        `{"type":"rodsadmin"}`,
			contentType: "application/json",
		},
		{
			name:   "delete user",
			method: http.MethodDelete,
			target: "/api/v1/user/bob",
		},
		{
			name:        "create user avu",
			method:      http.MethodPost,
			target:      "/api/v1/user/bob/avu",
			body:        `{"attrib":"department","value":"science"}`,
			contentType: "application/json",
		},
		{
			name:        "update user avu",
			method:      http.MethodPut,
			target:      "/api/v1/user/bob/avu/1",
			body:        `{"attrib":"department","value":"science"}`,
			contentType: "application/json",
		},
		{
			name:   "delete user avu",
			method: http.MethodDelete,
			target: "/api/v1/user/bob/avu/1",
		},
		{
			name:        "create usergroup",
			method:      http.MethodPost,
			target:      "/api/v1/usergroup",
			body:        `{"name":"science"}`,
			contentType: "application/json",
		},
		{
			name:   "delete usergroup",
			method: http.MethodDelete,
			target: "/api/v1/usergroup/research-team",
		},
		{
			name:        "create usergroup avu",
			method:      http.MethodPost,
			target:      "/api/v1/usergroup/research-team/avu",
			body:        `{"attrib":"purpose","value":"analysis"}`,
			contentType: "application/json",
		},
		{
			name:        "update usergroup avu",
			method:      http.MethodPut,
			target:      "/api/v1/usergroup/research-team/avu/1",
			body:        `{"attrib":"purpose","value":"analysis"}`,
			contentType: "application/json",
		},
		{
			name:   "delete usergroup avu",
			method: http.MethodDelete,
			target: "/api/v1/usergroup/research-team/avu/1",
		},
		{
			name:        "add usergroup member",
			method:      http.MethodPost,
			target:      "/api/v1/usergroup/research-team/member",
			body:        `{"user_name":"bob"}`,
			contentType: "application/json",
		},
		{
			name:   "remove usergroup member",
			method: http.MethodDelete,
			target: "/api/v1/usergroup/research-team/member/alice",
		},
		{
			name:        "create path ticket",
			method:      http.MethodPost,
			target:      "/api/v1/path/ticket?irods_path=/tempZone/home/test1/file.txt",
			body:        `{"maximum_uses":5}`,
			contentType: "application/json",
		},
		{
			name:        "create ticket",
			method:      http.MethodPost,
			target:      "/api/v1/ticket",
			body:        `{"irods_path":"/tempZone/home/test1/file.txt"}`,
			contentType: "application/json",
		},
		{
			name:        "update ticket",
			method:      http.MethodPatch,
			target:      "/api/v1/ticket/ticket-existing",
			body:        `{"maximum_uses":1}`,
			contentType: "application/json",
		},
		{
			name:   "delete ticket",
			method: http.MethodDelete,
			target: "/api/v1/ticket/ticket-existing",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var bodyReader io.Reader
			if tc.body != "" {
				bodyReader = strings.NewReader(tc.body)
			}

			req := httptest.NewRequest(tc.method, tc.target, bodyReader)
			if tc.contentType != "" {
				req.Header.Set("Content-Type", tc.contentType)
			}
			rec := httptest.NewRecorder()

			handler.Routes().ServeHTTP(rec, req)

			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("expected 401, got %d: %s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestAPIAcceptsValidBearerToken(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/path?irods_path=/tempZone/home/test1/file.txt", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	if body := rec.Body.String(); !containsAll(body, `"kind":"data_object"`) {
		t.Fatalf("unexpected response body: %q", body)
	}

	if body := rec.Body.String(); !containsAll(
		body,
		`"path_segments"`,
		`"display_name":"tempZone"`,
		`"display_name":"file.txt"`,
		`"/api/v1/path?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt"`,
	) {
		t.Fatalf("expected path segments in response body: %q", body)
	}

}

func TestGetServerInfo(t *testing.T) {
	handler, _ := testHandlerWithConfig(t, func(cfg *config.RestConfig) {
		cfg.IrodsHost = "irods.local"
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/server", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	if body := rec.Body.String(); !containsAll(
		body,
		`"server_info"`,
		`"release_version":"rods4.3.2"`,
		`"api_version":"d"`,
		`"reconnect_port":1247`,
		`"reconnect_addr":"irods.example.org"`,
		`"cookie":734`,
		`"irods_host":"irods.local"`,
		`"irods_port":1247`,
		`"irods_zone":"tempZone"`,
		`"irods_negotiation":"CS_NEG_DONT_CARE"`,
		`"irods_default_resource":"demoResc"`,
		`"resource_affinity":["demoResc","edgeResc"]`,
	) {
		t.Fatalf("unexpected response body: %q", body)
	}
}

func TestAPIAcceptsBasicAuth(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/path?irods_path=/tempZone/home/test1/file.txt", nil)
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("alice:secret")))
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestGetPathRequiresIRODSPathQuery(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/path", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestGetPathAcceptsValidBearerToken(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/path?irods_path=/tempZone/home/test1/file.txt", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	if body := rec.Body.String(); !containsAll(body, `"/tempZone/home/test1/file.txt"`, `"kind":"data_object"`) {
		t.Fatalf("unexpected response body: %q", body)
	}
	if body := rec.Body.String(); !containsAll(body, `"display_size":"128 B"`, `"mime_type":"text/plain; charset=utf-8"`, `"created_at":"2023-11-14T22:13:20Z"`, `"updated_at":"2023-11-14T22:13:20Z"`) {
		t.Fatalf("expected display size and timestamps in response body: %q", body)
	}
	if body := rec.Body.String(); !containsAll(
		body,
		`"self":{"href":"/api/v1/path?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"GET"}`,
		`"update":{"href":"/api/v1/path?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"PATCH"}`,
		`"delete":{"href":"/api/v1/path?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"DELETE"}`,
		`"move":{"href":"/api/v1/path?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"PATCH"}`,
		`"copy":{"href":"/api/v1/path?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"PATCH"}`,
		`"avus":{"href":"/api/v1/path/avu?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"GET"}`,
		`"replicas":{"href":"/api/v1/path/replicas?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"GET"}`,
		`"add_replica":{"href":"/api/v1/path/replicas?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"POST"}`,
		`"move_replica":{"href":"/api/v1/path/replicas?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"PATCH"}`,
		`"trim_replica":{"href":"/api/v1/path/replicas?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"DELETE"}`,
		`"download_contents":{"href":"/api/v1/path/contents?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"GET"}`,
		`"replace_contents":{"href":"/api/v1/path/contents?parent_path=%2FtempZone%2Fhome%2Ftest1\u0026file_name=file.txt","method":"POST"}`,
		`"create_avu":{"href":"/api/v1/path/avu?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"POST"}`,
		`"resource_link":{"href":"/api/v1/resource/demoResc","method":"GET"}`,
		`"cmd_cues":[`,
		`"operation":"put","gocmd":"gocmd put \u003cLOCAL_PATH\u003e '/tempZone/home/test1'"`,
		`"operation":"get","gocmd":"gocmd get '/tempZone/home/test1/file.txt' \u003cDESTINATION_PATH\u003e"`,
		`"operation":"phymove","icommand":"iphymv -S \u003csrcResource\u003e -R \u003ctargetResource\u003e '/tempZone/home/test1/file.txt'"`,
		`"operation":"replicate","icommand":"irepl -S \u003csrcResource\u003e -R \u003ctargetResource\u003e '/tempZone/home/test1/file.txt'"`,
	) {
		t.Fatalf("expected AVU HATEOAS link in response body: %q", body)
	}
}

func TestGetPathVerboseReturnsReplicaLongFormat(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/path?irods_path=/tempZone/home/test1/file.txt&verbose=1", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	if body := rec.Body.String(); !containsAll(
		body,
		`"replicas":[`,
		`"number":0`,
		`"owner":"rods"`,
		`"resource_name":"demoResc"`,
		`"resource_link":{"href":"/api/v1/resource/demoResc","method":"GET"}`,
		`"resource_hierarchy":"demoResc"`,
		`"status":"1"`,
		`"status_symbol":"\u0026"`,
		`"status_description":"good"`,
	) {
		t.Fatalf("expected ils -l style replica information in response body: %q", body)
	}
}

func TestGetPathVerboseReturnsReplicaVeryLongFormat(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/path?irods_path=/tempZone/home/test1/file.txt&verbose=2", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	if body := rec.Body.String(); !containsAll(
		body,
		`"checksum":{"checksum":"sha2:YWJjMTIz","type":"sha2"}`,
		`"data_type":"generic"`,
		`"physical_path":"/var/lib/irods/Vault/home/test1/file.txt"`,
	) {
		t.Fatalf("expected ils -L style replica information in response body: %q", body)
	}
}

func TestGetPathRejectsInvalidVerboseValue(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/path?irods_path=/tempZone/home/test1/file.txt&verbose=banana", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestGetPathReturnsCollectionShape(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/path?irods_path=/tempZone/home/test1/project", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	if body := rec.Body.String(); !containsAll(body, `"kind":"collection"`, `"childCount":2`) {
		t.Fatalf("unexpected collection response body: %q", body)
	}
	if body := rec.Body.String(); !containsAll(body, `"created_at":"2023-11-14T22:13:20Z"`, `"updated_at":"2023-11-14T22:13:20Z"`) {
		t.Fatalf("expected collection timestamps in response body: %q", body)
	}

	if body := rec.Body.String(); !containsAll(
		body,
		`"path_segments"`,
		`"display_name":"project"`,
		`"/api/v1/path?irods_path=%2FtempZone%2Fhome%2Ftest1%2Fproject"`,
		`"self":{"href":"/api/v1/path?irods_path=%2FtempZone%2Fhome%2Ftest1%2Fproject","method":"GET"}`,
		`"children":{"href":"/api/v1/path/children?irods_path=%2FtempZone%2Fhome%2Ftest1%2Fproject","method":"GET"}`,
		`"upload_contents":{"href":"/api/v1/path/contents?parent_path=%2FtempZone%2Fhome%2Ftest1%2Fproject","method":"POST"}`,
		`"move":{"href":"/api/v1/path?irods_path=%2FtempZone%2Fhome%2Ftest1%2Fproject","method":"PATCH"}`,
		`"copy":{"href":"/api/v1/path?irods_path=%2FtempZone%2Fhome%2Ftest1%2Fproject","method":"PATCH"}`,
		`"cmd_cues":[`,
		`"operation":"put","gocmd":"gocmd put -r \u003cLOCAL_PATH\u003e '/tempZone/home/test1/project'"`,
		`"operation":"get","gocmd":"gocmd get -r '/tempZone/home/test1/project' \u003cDESTINATION_PATH\u003e"`,
		`"operation":"phymove","icommand":"iphymv -r -S \u003csrcResource\u003e -R \u003ctargetResource\u003e '/tempZone/home/test1/project'"`,
		`"operation":"replicate","icommand":"irepl -r -S \u003csrcResource\u003e -R \u003ctargetResource\u003e '/tempZone/home/test1/project'"`,
		`"resources":{"href":"/api/v1/resource","method":"GET"}`,
		`"create_child_collection":{"href":"/api/v1/path?irods_path=%2FtempZone%2Fhome%2Ftest1%2Fproject","method":"POST"}`,
		`"create_child_data_object":{"href":"/api/v1/path?irods_path=%2FtempZone%2Fhome%2Ftest1%2Fproject","method":"POST"}`,
		`"set_inheritance":{"href":"/api/v1/path/acl/inheritance?irods_path=%2FtempZone%2Fhome%2Ftest1%2Fproject","method":"PUT"}`,
		`"delete_inheritance":{"href":"/api/v1/path/acl/inheritance?irods_path=%2FtempZone%2Fhome%2Ftest1%2Fproject","method":"DELETE"}`,
	) {
		t.Fatalf("expected path segments in collection response body: %q", body)
	}

}

func TestDeletePathDeletesDataObject(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/path?irods_path=/tempZone/home/test1/file.txt", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestDeletePathRejectsNonEmptyCollectionWithoutForce(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/path?irods_path=/tempZone/home/test1/project", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", rec.Code, rec.Body.String())
	}

	if body := rec.Body.String(); !containsAll(body, `"code":"conflict"`, `"message":"request conflicts with current resource state"`) {
		t.Fatalf("unexpected conflict response body: %q", body)
	}
}

func TestDeletePathDeletesCollectionRecursivelyWhenForced(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/path?irods_path=/tempZone/home/test1/project&force=true", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestPostPathMoveRenamesDataObject(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPatch, "/api/v1/path?irods_path=/tempZone/home/test1/file.txt", strings.NewReader(`{"new_name":"renamed.txt"}`))
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	if body := rec.Body.String(); !containsAll(body, `"path":"/tempZone/home/test1/renamed.txt"`, `"kind":"data_object"`) {
		t.Fatalf("unexpected rename response body: %q", body)
	}
}

func TestPostPathMoveRenamesCollection(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPatch, "/api/v1/path?irods_path=/tempZone/home/test1/project", strings.NewReader(`{"new_name":"renamed-project"}`))
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	if body := rec.Body.String(); !containsAll(body, `"path":"/tempZone/home/test1/renamed-project"`, `"kind":"collection"`) {
		t.Fatalf("unexpected rename collection response body: %q", body)
	}
}

func TestPatchPathMovesDataObjectToDestinationPath(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPatch, "/api/v1/path?irods_path=/tempZone/home/test1/file.txt", strings.NewReader(`{"operation":"move","destination_path":"/tempZone/home/test1/project/moved-file.txt"}`))
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	if body := rec.Body.String(); !containsAll(body, `"path":"/tempZone/home/test1/project/moved-file.txt"`, `"kind":"data_object"`) {
		t.Fatalf("unexpected move response body: %q", body)
	}
}

func TestPatchPathCopiesDataObjectToDestinationPath(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPatch, "/api/v1/path?irods_path=/tempZone/home/test1/file.txt", strings.NewReader(`{"operation":"copy","destination_path":"/tempZone/home/test1/project/copied-file.txt"}`))
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	if body := rec.Body.String(); !containsAll(body, `"path":"/tempZone/home/test1/project/copied-file.txt"`, `"kind":"data_object"`) {
		t.Fatalf("unexpected copy response body: %q", body)
	}
}

func TestPatchPathCopiesCollectionRecursively(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPatch, "/api/v1/path?irods_path=/tempZone/home/test1/project", strings.NewReader(`{"operation":"copy","destination_path":"/tempZone/home/test1/project-copy"}`))
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	if body := rec.Body.String(); !containsAll(body, `"path":"/tempZone/home/test1/project-copy"`, `"kind":"collection"`) {
		t.Fatalf("unexpected collection copy response body: %q", body)
	}
}

func TestGetPathChildrenReturnsCollectionChildren(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/path/children?irods_path=/tempZone/home/test1/project", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	if body := rec.Body.String(); !containsAll(body, `"display_size":"64 B"`, `"created_at":"2023-11-14T22:13:20Z"`, `"updated_at":"2023-11-14T22:13:20Z"`) {
		t.Fatalf("expected child timestamps and display size in response body: %q", body)
	}

	if body := rec.Body.String(); !containsAll(body, `"children"`, `"kind":"data_object"`, `"kind":"collection"`) {
		t.Fatalf("unexpected children response body: %q", body)
	}
	if body := rec.Body.String(); !containsAll(
		body,
		`"links":{"self":{"href":"/api/v1/path/children?irods_path=/tempZone/home/test1/project","method":"GET"}`,
		`"parent":{"href":"/api/v1/path?irods_path=%2FtempZone%2Fhome%2Ftest1","method":"GET"}`,
		`"create_child_collection":{"href":"/api/v1/path?irods_path=%2FtempZone%2Fhome%2Ftest1%2Fproject","method":"POST"}`,
		`"upload_contents":{"href":"/api/v1/path/contents?parent_path=%2FtempZone%2Fhome%2Ftest1%2Fproject","method":"POST"}`,
	) {
		t.Fatalf("expected children-level links in response body: %q", body)
	}
	if body := rec.Body.String(); !containsAll(
		body,
		`"details":{"href":"/api/v1/path?irods_path=%2FtempZone%2Fhome%2Ftest1%2Fproject%2Fchild.txt","method":"GET"}`,
		`"move":{"href":"/api/v1/path?irods_path=%2FtempZone%2Fhome%2Ftest1%2Fproject%2Fchild.txt","method":"PATCH"}`,
		`"copy":{"href":"/api/v1/path?irods_path=%2FtempZone%2Fhome%2Ftest1%2Fproject%2Fchild.txt","method":"PATCH"}`,
	) {
		t.Fatalf("expected per-child action links in response body: %q", body)
	}

	if body := rec.Body.String(); !containsAll(
		body,
		`"path_segments"`,
		`"display_name":"tempZone"`,
		`"irods_path":"/tempZone/home/test1/project"`,
		`"/api/v1/path?irods_path=%2FtempZone%2Fhome%2Ftest1%2Fproject"`,
	) {
		t.Fatalf("expected path segments in response body: %q", body)
	}
}

func TestPostPathChildrenEndpointRemoved(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/path/children?irods_path=/tempZone/home/test1/project", strings.NewReader(`{"child_name":"alias.txt","kind":"data_object"}`))
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestGetPathChildrenSearchMatchesRelativePatternAndIncludesMetadata(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/path/children?irods_path=/tempZone/home/test1/project&name_pattern=/child*", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	if body := rec.Body.String(); !containsAll(
		body,
		`"children":[`,
		`"/tempZone/home/test1/project/child.txt"`,
		`"search":{"case_sensitive":true,"matched_count":1,"name_pattern":"child*","recursive":false,"search_scope":"children"}`,
	) {
		t.Fatalf("unexpected search response body: %q", body)
	}
}

func TestGetPathChildrenSearchSubtreeViaSearchScope(t *testing.T) {
	handler, filesystem := testHandlerWithConfig(t, nil)

	now := time.Unix(1_700_000_007, 0)
	nestedFile := &irodsfs.Entry{
		ID:         104,
		Type:       irodsfs.FileEntry,
		Name:       "child2.txt",
		Path:       "/tempZone/home/test1/project/nested/child2.txt",
		Owner:      "alice",
		Size:       16,
		DataType:   "generic",
		CreateTime: now,
		ModifyTime: now,
	}
	filesystem.entriesByPath[nestedFile.Path] = nestedFile
	filesystem.childrenByPath["/tempZone/home/test1/project/nested"] = []*irodsfs.Entry{nestedFile}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/path/children?irods_path=/tempZone/home/test1/project&name_pattern=child*&search_scope=subtree", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	if body := rec.Body.String(); !containsAll(
		body,
		`"/tempZone/home/test1/project/child.txt"`,
		`"/tempZone/home/test1/project/nested/child2.txt"`,
		`"search":{"case_sensitive":true,"matched_count":2,"name_pattern":"child*","recursive":true,"search_scope":"subtree"}`,
	) {
		t.Fatalf("unexpected subtree search response body: %q", body)
	}
}

func TestGetPathChildrenSearchRejectsInvalidScope(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/path/children?irods_path=/tempZone/home/test1/project&name_pattern=child*&search_scope=global", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestGetPathChildrenSearchRejectsRecursiveAlias(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/path/children?irods_path=/tempZone/home/test1/project&name_pattern=child*&recursive=true", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestPostPathQueryAVUConditionsReturnPagedUnifiedPaths(t *testing.T) {
	handler := testHandler(t)

	body := `{"irods_path":"/tempZone/home/test1","search_scope":"children","kinds":["data_object","collection"],"conditions":[{"field":"avu.attrib","op":"=","value":"source"},{"field":"avu.value","op":"=","value":"test"}],"limit":1,"include_matched_avus":true}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/path/query", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var firstPage struct {
		Paths []domain.PathEntry `json:"paths"`
		Page  struct {
			HasMore       bool   `json:"has_more"`
			NextPageToken string `json:"next_page_token"`
		} `json:"page"`
		MatchedAVUs map[string][]domain.AVUMetadata `json:"matched_avus"`
		Query       struct {
			SearchScope string `json:"search_scope"`
			Scope       struct {
				Root string `json:"root"`
				Mode string `json:"mode"`
			} `json:"scope"`
		} `json:"query"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &firstPage); err != nil {
		t.Fatalf("decode first query page: %v", err)
	}
	if len(firstPage.Paths) != 1 {
		t.Fatalf("expected one path on first page, got %+v", firstPage.Paths)
	}
	if firstPage.Paths[0].Path != "/tempZone/home/test1/project" || firstPage.Paths[0].Kind != "collection" {
		t.Fatalf("unexpected first page path: %+v", firstPage.Paths[0])
	}
	if !firstPage.Page.HasMore || strings.TrimSpace(firstPage.Page.NextPageToken) == "" {
		t.Fatalf("expected first page to include a next page token: %+v", firstPage.Page)
	}
	if len(firstPage.MatchedAVUs[firstPage.Paths[0].Path]) != 1 {
		t.Fatalf("expected matched AVU details for first path, got %+v", firstPage.MatchedAVUs)
	}
	matchedAVU := firstPage.MatchedAVUs[firstPage.Paths[0].Path][0]
	if matchedAVU.ID != "700" {
		t.Fatalf("expected matched AVU id 700, got %+v", matchedAVU)
	}
	if matchedAVU.Links == nil || matchedAVU.Links.Update == nil || matchedAVU.Links.Delete == nil {
		t.Fatalf("expected matched AVU HATEOAS update/delete links, got %+v", matchedAVU)
	}
	if matchedAVU.Links.Update.Href != "/api/v1/path/avu/700?irods_path=%2FtempZone%2Fhome%2Ftest1%2Fproject" || matchedAVU.Links.Update.Method != http.MethodPut {
		t.Fatalf("unexpected matched AVU update link: %+v", matchedAVU.Links.Update)
	}
	if firstPage.Query.SearchScope != "children" || firstPage.Query.Scope.Root != "/tempZone/home/test1" || firstPage.Query.Scope.Mode != "children" {
		t.Fatalf("expected query summary to include scope mode and root, got %+v", firstPage.Query)
	}

	secondBody := `{"irods_path":"/tempZone/home/test1","search_scope":"children","kinds":["data_object","collection"],"conditions":[{"field":"avu.attrib","op":"=","value":"source"},{"field":"avu.value","op":"=","value":"test"}],"limit":1,"include_matched_avus":true,"page_token":"` + firstPage.Page.NextPageToken + `"}`
	req = httptest.NewRequest(http.MethodPost, "/api/v1/path/query", strings.NewReader(secondBody))
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for second page, got %d: %s", rec.Code, rec.Body.String())
	}

	var secondPage struct {
		Paths []domain.PathEntry `json:"paths"`
		Page  struct {
			HasMore bool `json:"has_more"`
		} `json:"page"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &secondPage); err != nil {
		t.Fatalf("decode second query page: %v", err)
	}
	if len(secondPage.Paths) != 1 || secondPage.Paths[0].Path != "/tempZone/home/test1/file.txt" {
		t.Fatalf("expected second page file match, got %+v", secondPage.Paths)
	}
	if secondPage.Page.HasMore {
		t.Fatalf("expected second page to exhaust results: %+v", secondPage.Page)
	}
	if len(secondPage.Paths[0].Replicas) != 0 {
		t.Fatalf("expected query path response to omit replicas by default, got %+v", secondPage.Paths[0].Replicas)
	}
}

func TestPostPathQuerySupportsFileConditions(t *testing.T) {
	handler := testHandler(t)

	body := `{"irods_path":"/tempZone/home/test1/project","search_scope":"children","kinds":["data_object"],"conditions":[{"field":"name","op":"like","value":"child*"}]}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/path/query", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if body := rec.Body.String(); !containsAll(body, `"paths":[`, `"/tempZone/home/test1/project/child.txt"`, `"kind":"data_object"`, `"search_scope":"children"`) {
		t.Fatalf("unexpected path query response body: %q", body)
	}
	if body := rec.Body.String(); !containsAll(body, `"scope":{"root":"/tempZone/home/test1/project","mode":"children"`) {
		t.Fatalf("expected path query response to include scope root: %q", body)
	}
}

func TestPostPathQueryRejectsInvalidPageToken(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/path/query", strings.NewReader(`{"page_token":"not valid"}`))
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestGetPathReplicasRequiresIRODSPathQuery(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/path/replicas", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestGetPathReplicasReturnsReplicaList(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/path/replicas?irods_path=/tempZone/home/test1/file.txt&verbose=2", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	if body := rec.Body.String(); !containsAll(
		body,
		`"irods_path":"/tempZone/home/test1/file.txt"`,
		`"links":{"self":{"href":"/api/v1/path/replicas?irods_path=/tempZone/home/test1/file.txt\u0026verbose=2","method":"GET"}`,
		`"add_replica":{"href":"/api/v1/path/replicas?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"POST"}`,
		`"move_replica":{"href":"/api/v1/path/replicas?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"PATCH"}`,
		`"trim_replica":{"href":"/api/v1/path/replicas?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"DELETE"}`,
		`"replicas":[`,
		`"trim":{"href":"/api/v1/path/replicas?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"DELETE"}`,
		`"resource_details":{"href":"/api/v1/resource/demoResc","method":"GET"}`,
		`"resource_name":"demoResc"`,
		`"resource_link":{"href":"/api/v1/resource/demoResc","method":"GET"}`,
	) {
		t.Fatalf("unexpected response body: %q", body)
	}
}

func TestPostPathReplicasCreatesReplica(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/path/replicas?irods_path=/tempZone/home/test1/file.txt", strings.NewReader(`{"resource":"archiveResc","update":true}`))
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}

	if body := rec.Body.String(); !containsAll(
		body,
		`"resource_name":"demoResc"`,
		`"resource_name":"archiveResc"`,
	) {
		t.Fatalf("expected created replica in response body: %q", body)
	}
}

func TestPostPathReplicasRejectsMissingResource(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/path/replicas?irods_path=/tempZone/home/test1/file.txt", strings.NewReader(`{"update":true}`))
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}

	if body := rec.Body.String(); !containsAll(body, `"fields":{"resource":"resource is required"}`) {
		t.Fatalf("expected field validation error, got %q", body)
	}
}

func TestPatchPathReplicasMovesReplica(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPatch, "/api/v1/path/replicas?irods_path=/tempZone/home/test1/file.txt", strings.NewReader(`{"source_resource":"demoResc","destination_resource":"archiveResc","update":true,"min_copies":1}`))
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	if body := rec.Body.String(); !containsAll(body, `"resource_name":"archiveResc"`) {
		t.Fatalf("expected destination replica in response body: %q", body)
	}
	if strings.Contains(rec.Body.String(), `"resource_name":"demoResc"`) {
		t.Fatalf("expected source replica to be trimmed, got %q", rec.Body.String())
	}
}

func TestPatchPathReplicasRejectsSameSourceAndDestination(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPatch, "/api/v1/path/replicas?irods_path=/tempZone/home/test1/file.txt", strings.NewReader(`{"source_resource":"demoResc","destination_resource":"demoResc"}`))
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}

	if body := rec.Body.String(); !containsAll(body, `"destination_resource":"destination_resource must differ from source_resource"`) {
		t.Fatalf("expected destination field validation error, got %q", body)
	}
}

func TestDeletePathReplicasTrimsReplicaByNumber(t *testing.T) {
	handler := testHandler(t)

	createReq := httptest.NewRequest(http.MethodPost, "/api/v1/path/replicas?irods_path=/tempZone/home/test1/file.txt", strings.NewReader(`{"resource":"archiveResc","update":true}`))
	createReq.Header.Set("Authorization", "Bearer token123")
	createReq.Header.Set("Content-Type", "application/json")
	createRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("expected create status 201, got %d: %s", createRec.Code, createRec.Body.String())
	}

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/path/replicas?irods_path=/tempZone/home/test1/file.txt", strings.NewReader(`{"replica_number":1,"min_copies":1}`))
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	if body := rec.Body.String(); !containsAll(body, `"resource_name":"demoResc"`) {
		t.Fatalf("expected default replica to remain in response body: %q", body)
	}
	if strings.Contains(rec.Body.String(), `"resource_name":"archiveResc"`) {
		t.Fatalf("expected archive replica to be trimmed, got %q", rec.Body.String())
	}
}

func TestDeletePathReplicasRejectsMissingSelector(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/path/replicas?irods_path=/tempZone/home/test1/file.txt", strings.NewReader(`{"min_copies":1}`))
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}

	if body := rec.Body.String(); !containsAll(
		body,
		`"fields":{`,
		`"resource":"resource or replica_number is required"`,
		`"replica_number":"resource or replica_number is required"`,
	) {
		t.Fatalf("expected validation errors for selector fields, got %q", body)
	}
}

func TestPatchPathReplicasUsesConfiguredTrimDefaultsWhenOmitted(t *testing.T) {
	handler, filesystem := testHandlerWithConfig(t, func(cfg *config.RestConfig) {
		cfg.ReplicaTrimMinCopies = 1
		cfg.ReplicaTrimMinAgeMinutes = 17
	})

	req := httptest.NewRequest(http.MethodPatch, "/api/v1/path/replicas?irods_path=/tempZone/home/test1/file.txt", strings.NewReader(`{"source_resource":"demoResc","destination_resource":"archiveResc","update":true}`))
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if filesystem.lastTrimMinCopies != 1 {
		t.Fatalf("expected configured trim min copies 1, got %d", filesystem.lastTrimMinCopies)
	}
	if filesystem.lastTrimMinAgeMins != 17 {
		t.Fatalf("expected configured trim min age minutes 17, got %d", filesystem.lastTrimMinAgeMins)
	}
}

func TestDeletePathReplicasUsesConfiguredTrimDefaultsWhenOmitted(t *testing.T) {
	handler, filesystem := testHandlerWithConfig(t, func(cfg *config.RestConfig) {
		cfg.ReplicaTrimMinCopies = 1
		cfg.ReplicaTrimMinAgeMinutes = 23
	})

	createReq := httptest.NewRequest(http.MethodPost, "/api/v1/path/replicas?irods_path=/tempZone/home/test1/file.txt", strings.NewReader(`{"resource":"archiveResc","update":true}`))
	createReq.Header.Set("Authorization", "Bearer token123")
	createReq.Header.Set("Content-Type", "application/json")
	createRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("expected create status 201, got %d: %s", createRec.Code, createRec.Body.String())
	}

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/path/replicas?irods_path=/tempZone/home/test1/file.txt", strings.NewReader(`{"resource":"demoResc"}`))
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if filesystem.lastTrimMinCopies != 1 {
		t.Fatalf("expected configured trim min copies 1, got %d", filesystem.lastTrimMinCopies)
	}
	if filesystem.lastTrimMinAgeMins != 23 {
		t.Fatalf("expected configured trim min age minutes 23, got %d", filesystem.lastTrimMinAgeMins)
	}
}

func TestPostPathChildrenCreatesCollection(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/path?irods_path=/tempZone/home/test1/project", strings.NewReader(`{"child_name":"new-folder","kind":"collection"}`))
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}

	if body := rec.Body.String(); !containsAll(
		body,
		`"path":"/tempZone/home/test1/project/new-folder"`,
		`"kind":"collection"`,
		`"create_child_collection":{"href":"/api/v1/path?irods_path=%2FtempZone%2Fhome%2Ftest1%2Fproject%2Fnew-folder","method":"POST"}`,
	) {
		t.Fatalf("unexpected create collection response body: %q", body)
	}
}

func TestPostPathChildrenCreatesZeroByteFile(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/path?irods_path=/tempZone/home/test1/project", strings.NewReader(`{"child_name":"empty.txt","kind":"data_object"}`))
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}

	if body := rec.Body.String(); !containsAll(
		body,
		`"path":"/tempZone/home/test1/project/empty.txt"`,
		`"kind":"data_object"`,
		`"display_size":"0 B"`,
	) {
		t.Fatalf("unexpected create data object response body: %q", body)
	}
}

func TestPostPathContentsUploadsDataObject(t *testing.T) {
	handler := testHandler(t)

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	if err := writer.WriteField("parent_path", "/tempZone/home/test1/project"); err != nil {
		t.Fatalf("write parent_path: %v", err)
	}
	if err := writer.WriteField("file_name", "upload.txt"); err != nil {
		t.Fatalf("write file_name: %v", err)
	}
	if err := writer.WriteField("checksum", "true"); err != nil {
		t.Fatalf("write checksum: %v", err)
	}
	part, err := writer.CreateFormFile("content", "upload.txt")
	if err != nil {
		t.Fatalf("create content part: %v", err)
	}
	if _, err := part.Write([]byte("upload payload")); err != nil {
		t.Fatalf("write content part: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/path/contents", &body)
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Content-Type", writer.FormDataContentType())
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}

	if body := rec.Body.String(); !containsAll(
		body,
		`"path":"/tempZone/home/test1/project/upload.txt"`,
		`"parent_path":"/tempZone/home/test1/project"`,
		`"file_name":"upload.txt"`,
		`"action":"created"`,
		`"size":14`,
		`"requested":true`,
		`"verified":true`,
		`"path":{"href":"/api/v1/path?irods_path=%2FtempZone%2Fhome%2Ftest1%2Fproject%2Fupload.txt","method":"GET"}`,
		`"contents":{"href":"/api/v1/path/contents?irods_path=%2FtempZone%2Fhome%2Ftest1%2Fproject%2Fupload.txt","method":"GET"}`,
		`"parent":{"href":"/api/v1/path?irods_path=%2FtempZone%2Fhome%2Ftest1%2Fproject","method":"GET"}`,
	) {
		t.Fatalf("unexpected upload response body: %q", body)
	}
}

func TestPostPathChildrenRejectsMkdirsForDataObject(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/path?irods_path=/tempZone/home/test1/project", strings.NewReader(`{"child_name":"nested/empty.txt","kind":"data_object","mkdirs":true}`))
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}

	if body := rec.Body.String(); !containsAll(body, `"message":"mkdirs is only supported for collection creation"`) {
		t.Fatalf("unexpected validation response body: %q", body)
	}
}

func TestGetPathACLReturnsUsersGroupsAndLinks(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/path/acl?irods_path=/tempZone/home/test1/file.txt", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	if body := rec.Body.String(); !containsAll(
		body,
		`"irods_path":"/tempZone/home/test1/file.txt"`,
		`"kind":"data_object"`,
		`"path_segments"`,
		`"path":{"href":"/api/v1/path?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"GET"}`,
		`"add_user":{"href":"/api/v1/path/acl?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"POST"}`,
		`"users":[`,
		`"id":"user:tempZone:alice"`,
		`"name":"alice"`,
		`"type":"user"`,
		`"irods_user_type":"rodsuser"`,
		`"access_level":"own"`,
		`"groups":[`,
		`"id":"group:tempZone:research-team"`,
		`"name":"research-team"`,
		`"type":"group"`,
		`"irods_user_type":"rodsgroup"`,
		`"access_level":"read_object"`,
		`"update":{"href":"/api/v1/path/acl/user:tempZone:alice?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"PUT"}`,
		`"remove":{"href":"/api/v1/path/acl/group:tempZone:research-team?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"DELETE"}`,
	) {
		t.Fatalf("unexpected ACL response body: %q", body)
	}
}

func TestGetPathACLRequiresIRODSPathQuery(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/path/acl", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestGetCollectionPathACLIncludesInheritanceControls(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/path/acl?irods_path=/tempZone/home/test1/project", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if body := rec.Body.String(); !containsAll(
		body,
		`"kind":"collection"`,
		`"inheritance_enabled":false`,
		`"set_inheritance":{"href":"/api/v1/path/acl/inheritance?irods_path=%2FtempZone%2Fhome%2Ftest1%2Fproject","method":"PUT"}`,
	) {
		t.Fatalf("unexpected collection ACL response body: %q", body)
	}
}

func TestPutPathACLInheritanceEnablesCollectionInheritance(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/path/acl/inheritance?irods_path=/tempZone/home/test1/project", strings.NewReader(`{"enabled":true,"recursive":true}`))
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d: %s", rec.Code, rec.Body.String())
	}

	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/path/acl?irods_path=/tempZone/home/test1/project", nil)
	getReq.Header.Set("Authorization", "Bearer token123")
	getRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", getRec.Code, getRec.Body.String())
	}
	if !strings.Contains(getRec.Body.String(), `"inheritance_enabled":true`) {
		t.Fatalf("expected inheritance enabled in response, got %q", getRec.Body.String())
	}
}

func TestDeletePathACLInheritanceDisablesCollectionInheritance(t *testing.T) {
	handler := testHandler(t)

	enableReq := httptest.NewRequest(http.MethodPut, "/api/v1/path/acl/inheritance?irods_path=/tempZone/home/test1/project", strings.NewReader(`{"enabled":true}`))
	enableReq.Header.Set("Authorization", "Bearer token123")
	enableReq.Header.Set("Content-Type", "application/json")
	enableRec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(enableRec, enableReq)

	if enableRec.Code != http.StatusNoContent {
		t.Fatalf("expected 204 while enabling inheritance, got %d: %s", enableRec.Code, enableRec.Body.String())
	}

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/path/acl/inheritance?irods_path=/tempZone/home/test1/project&recursive=true", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d: %s", rec.Code, rec.Body.String())
	}

	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/path/acl?irods_path=/tempZone/home/test1/project", nil)
	getReq.Header.Set("Authorization", "Bearer token123")
	getRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", getRec.Code, getRec.Body.String())
	}
	if !strings.Contains(getRec.Body.String(), `"inheritance_enabled":false`) {
		t.Fatalf("expected inheritance disabled in response, got %q", getRec.Body.String())
	}
}

func TestPostPathACLAddsPermission(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/path/acl?irods_path=/tempZone/home/test1/project", strings.NewReader(`{"name":"bob","type":"user","zone":"tempZone","access_level":"read_object"}`))
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}
	if body := rec.Body.String(); !containsAll(body, `"acl":{"id":"user:tempZone:bob"`, `"name":"bob"`, `"access_level":"read_object"`) {
		t.Fatalf("unexpected ACL create response body: %q", body)
	}
}

func TestPutPathACLUpdatesPermission(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/path/acl/group:tempZone:research-team?irods_path=/tempZone/home/test1/file.txt", strings.NewReader(`{"access_level":"modify_object","recursive":true}`))
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if body := rec.Body.String(); !containsAll(body, `"acl":{"id":"group:tempZone:research-team"`, `"access_level":"modify_object"`) {
		t.Fatalf("unexpected ACL update response body: %q", body)
	}
}

func TestDeletePathACLRemovesPermission(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/path/acl/group:tempZone:research-team?irods_path=/tempZone/home/test1/file.txt", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d: %s", rec.Code, rec.Body.String())
	}

	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/path/acl?irods_path=/tempZone/home/test1/file.txt", nil)
	getReq.Header.Set("Authorization", "Bearer token123")
	getRec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", getRec.Code, getRec.Body.String())
	}
	if strings.Contains(getRec.Body.String(), `"name":"research-team"`) {
		t.Fatalf("expected research-team ACL to be removed, got %q", getRec.Body.String())
	}
}

func TestGetPathAVUsReturnsAVUList(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/path/avu?irods_path=/tempZone/home/test1/file.txt", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	if body := rec.Body.String(); !containsAll(
		body,
		`"avus"`,
		`"id":"701"`,
		`"attrib":"source"`,
		`"value":"test"`,
		`"unit":"fixture"`,
		`"links":{"self":{"href":"/api/v1/path?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"GET"}`,
		`"details":{"href":"/api/v1/path?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"GET"}`,
		`"update":{"href":"/api/v1/path?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"PATCH"}`,
		`"avus":{"href":"/api/v1/path/avu?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"GET"}`,
		`"create_ticket":{"href":"/api/v1/path/ticket?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"POST"}`,
		`"update":{"href":"/api/v1/path/avu/701?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"PUT"}`,
		`"delete":{"href":"/api/v1/path/avu/701?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"DELETE"}`,
		`"count":1`,
		`"total":1`,
		`"created_at":"2023-11-14T22:13:20Z"`,
		`"updated_at":"2023-11-14T22:13:20Z"`,
		`"path_segments"`,
		`"/api/v1/path?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt"`,
	) {
		t.Fatalf("unexpected metadata response body: %q", body)
	}
}

func TestGetPathAVUsSupportsFilterSortAndPagination(t *testing.T) {
	handler := testHandler(t)

	for _, body := range []string{
		`{"attrib":"priority","value":"2","unit":"test"}`,
		`{"attrib":"priority","value":"1","unit":"test"}`,
	} {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/path/avu?irods_path=/tempZone/home/test1/file.txt", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer token123")
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		handler.Routes().ServeHTTP(rec, req)

		if rec.Code != http.StatusCreated {
			t.Fatalf("expected 201 while seeding AVU, got %d: %s", rec.Code, rec.Body.String())
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/path/avu?irods_path=/tempZone/home/test1/file.txt&attrib=priority&sort=value&order=asc&limit=1", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	body := rec.Body.String()
	if !containsAll(body, `"value":"1"`, `"count":1`, `"total":2`, `"offset":0`, `"limit":1`) {
		t.Fatalf("unexpected filtered AVU response body: %q", body)
	}
	if strings.Contains(body, `"value":"2"`) || strings.Contains(body, `"attrib":"source"`) {
		t.Fatalf("expected filtered and paginated AVU response, got %q", body)
	}
}

func TestPostPathAVUReturnsFieldValidationErrors(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/path/avu?irods_path=/tempZone/home/test1/file.txt", strings.NewReader(`{"attrib":" ","value":""}`))
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}

	if body := rec.Body.String(); !containsAll(
		body,
		`"message":"AVU request validation failed"`,
		`"fields":{"attrib":"attribute is required","value":"value is required"}`,
	) {
		t.Fatalf("unexpected validation response body: %q", body)
	}
}

func TestPostPathAVUCreatesAVU(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/path/avu?irods_path=/tempZone/home/test1/file.txt", strings.NewReader(`{"attrib":"new-attr","value":"new-value","unit":"new-unit"}`))
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rec.Code)
	}

	if body := rec.Body.String(); !containsAll(
		body,
		`"avu"`,
		`"attrib":"new-attr"`,
		`"value":"new-value"`,
		`"unit":"new-unit"`,
		`"update":{"href":"/api/v1/path/avu/702?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"PUT"}`,
		`"delete":{"href":"/api/v1/path/avu/702?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"DELETE"}`,
	) {
		t.Fatalf("unexpected create AVU response body: %q", body)
	}
}

func TestPutPathAVUUpdatesAVU(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/path/avu/701?irods_path=/tempZone/home/test1/file.txt", strings.NewReader(`{"attrib":"source","value":"updated","unit":"fixture"}`))
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	if body := rec.Body.String(); !containsAll(body, `"attrib":"source"`, `"value":"updated"`, `"unit":"fixture"`) {
		t.Fatalf("unexpected update AVU response body: %q", body)
	}
}

func TestDeletePathAVURemovesAVU(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/path/avu/701?irods_path=/tempZone/home/test1/file.txt", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/path/avu?irods_path=/tempZone/home/test1/file.txt", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec = httptest.NewRecorder()
	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 from AVU list after delete, got %d", rec.Code)
	}
	if body := rec.Body.String(); strings.Contains(body, `"id":"701"`) {
		t.Fatalf("expected AVU 701 to be removed, got %q", body)
	}
}

func TestUserAVULifecycle(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/user/alice/avu?zone=tempZone", strings.NewReader(`{"attrib":"department","value":"science","unit":"starbase"}`))
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}
	if body := rec.Body.String(); !containsAll(
		body,
		`"user_name":"alice"`,
		`"zone":"tempZone"`,
		`"attrib":"department"`,
		`"value":"science"`,
		`"unit":"starbase"`,
		`"update":{"href":"/api/v1/user/alice/avu/1?zone=tempZone","method":"PUT"}`,
		`"delete":{"href":"/api/v1/user/alice/avu/1?zone=tempZone","method":"DELETE"}`,
	) {
		t.Fatalf("unexpected user AVU create response body: %q", body)
	}

	req = httptest.NewRequest(http.MethodPut, "/api/v1/user/alice/avu/1?zone=tempZone", strings.NewReader(`{"attrib":"department","value":"operations","unit":"starbase"}`))
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if body := rec.Body.String(); !containsAll(body, `"attrib":"department"`, `"value":"operations"`, `"unit":"starbase"`) {
		t.Fatalf("unexpected user AVU update response body: %q", body)
	}

	req = httptest.NewRequest(http.MethodDelete, "/api/v1/user/alice/avu/1?zone=tempZone", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec = httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d: %s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/user/alice/avu?zone=tempZone", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec = httptest.NewRecorder()
	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !containsAll(
		body,
		`"links":{"create":{"href":"/api/v1/user/alice/avu?zone=tempZone","method":"POST"}`,
		`"self":{"href":"/api/v1/user/alice/avu?zone=tempZone","method":"GET"}`,
	) {
		t.Fatalf("unexpected user AVU list links: %q", body)
	}
	if strings.Contains(body, `"attrib":"department"`) {
		t.Fatalf("expected user AVU to be removed, got %q", body)
	}
}

func TestUserGroupAVUsCanBeCreatedAndListed(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/usergroup/research-team/avu?zone=tempZone", strings.NewReader(`{"attrib":"purpose","value":"analysis","unit":"starbase"}`))
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}
	if body := rec.Body.String(); !containsAll(
		body,
		`"group_name":"research-team"`,
		`"attrib":"purpose"`,
		`"value":"analysis"`,
		`"update":{"href":"/api/v1/usergroup/research-team/avu/1?zone=tempZone","method":"PUT"}`,
		`"delete":{"href":"/api/v1/usergroup/research-team/avu/1?zone=tempZone","method":"DELETE"}`,
	) {
		t.Fatalf("unexpected group AVU create response body: %q", body)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/usergroup/research-team/avu?zone=tempZone&attrib=purpose", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec = httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if body := rec.Body.String(); !containsAll(
		body,
		`"group_name":"research-team"`,
		`"count":1`,
		`"total":1`,
		`"attrib":"purpose"`,
		`"value":"analysis"`,
		`"links":{"create":{"href":"/api/v1/usergroup/research-team/avu?zone=tempZone","method":"POST"}`,
		`"self":{"href":"/api/v1/usergroup/research-team/avu?zone=tempZone","method":"GET"}`,
		`"update":{"href":"/api/v1/usergroup/research-team/avu/1?zone=tempZone","method":"PUT"}`,
		`"delete":{"href":"/api/v1/usergroup/research-team/avu/1?zone=tempZone","method":"DELETE"}`,
	) {
		t.Fatalf("unexpected group AVU list response body: %q", body)
	}
}

func TestGetPathChecksumReturnsTypedChecksum(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/path/checksum?irods_path=/tempZone/home/test1/file.txt", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	if body := rec.Body.String(); !containsAll(
		body,
		`"irods_path":"/tempZone/home/test1/file.txt"`,
		`"checksum":"sha2:YWJjMTIz"`,
		`"type":"sha2"`,
		`"path_segments"`,
	) {
		t.Fatalf("unexpected checksum response body: %q", body)
	}
}

func TestPostPathChecksumComputesAndUpdatesPathView(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/path/checksum?irods_path=/tempZone/home/test1/project/child.txt", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	if body := rec.Body.String(); !containsAll(body, `"checksum":"sha2:Y2hpbGQtY29tcHV0ZWQ="`, `"type":"sha2"`) {
		t.Fatalf("unexpected computed checksum response body: %q", body)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/path?irods_path=/tempZone/home/test1/project/child.txt", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec = httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 from path lookup after checksum compute, got %d", rec.Code)
	}

	if body := rec.Body.String(); !containsAll(body, `"checksum":{"checksum":"sha2:Y2hpbGQtY29tcHV0ZWQ=","type":"sha2"}`) {
		t.Fatalf("expected path response to reflect computed checksum, got %q", body)
	}
}

func TestGetPathReturnsForbiddenForPermissionDenied(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/path?irods_path=/tempZone/home/test1/forbidden", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", rec.Code)
	}

	if body := rec.Body.String(); !containsAll(body, `"code":"permission_denied"`) {
		t.Fatalf("unexpected forbidden response body: %q", body)
	}
}

func TestGetPathContentsAcceptsIRODSTicketBearer(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/path/contents?irods_path=/tempZone/home/test1/file.txt", nil)
	req.Header.Set("Authorization", "Bearer irods-ticket:ticket123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	if got := rec.Header().Get("Accept-Ranges"); got != "bytes" {
		t.Fatalf("expected Accept-Ranges header, got %q", got)
	}
	if got := rec.Header().Get("Content-Disposition"); !containsAll(got, `attachment`, `filename="file.txt"`) {
		t.Fatalf("expected download Content-Disposition header, got %q", got)
	}
	if got := rec.Header().Get("ETag"); got != `"sha2:YWJjMTIz"` {
		t.Fatalf("expected ETag header, got %q", got)
	}
	if got := rec.Header().Get("Last-Modified"); got == "" {
		t.Fatal("expected Last-Modified header")
	}
	if got := rec.Header().Get("X-Content-Type-Options"); got != "nosniff" {
		t.Fatalf("expected X-Content-Type-Options nosniff, got %q", got)
	}

	if body := rec.Body.String(); body != "hello content payload" {
		t.Fatalf("unexpected content body %q", body)
	}
}

func TestGetPathContentsAcceptsTicketIDQueryWithoutAuthorization(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/path/contents?irods_path=/tempZone/home/test1/file.txt&ticket_id=ticket123", nil)
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if body := rec.Body.String(); body != "hello content payload" {
		t.Fatalf("unexpected content body %q", body)
	}
}

func TestGetPathContentsAcceptsBasicAuth(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/path/contents?irods_path=/tempZone/home/test1/file.txt", nil)
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("alice:secret")))
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestPostPathTicketCreatesAnonymousTicket(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/path/ticket?irods_path=/tempZone/home/test1/file.txt", strings.NewReader(`{"maximum_uses":5,"lifetime_minutes":30}`))
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rec.Code)
	}
	if body := rec.Body.String(); !containsAll(body, `"ticket":{"name":"ticket_`, `"bearer_token":"irods-ticket:ticket_`, `"irods_path":"/tempZone/home/test1/file.txt"`, `"download":{"href":"/api/v1/path/contents?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt\u0026ticket_id=ticket_`, `"method":"GET"}`) {
		t.Fatalf("unexpected response body: %q", body)
	}
}

func TestGetTicketsReturnsOwnedTickets(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/ticket", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if body := rec.Body.String(); !containsAll(
		body,
		`"tickets":[`,
		`"name":"ticket-existing"`,
		`"self":{"href":"/api/v1/ticket/ticket-existing","method":"GET"}`,
		`"path":{"href":"/api/v1/path?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"GET"}`,
		`"create":{"href":"/api/v1/ticket","method":"POST"}`,
	) {
		t.Fatalf("unexpected response body: %q", body)
	}
}

func TestGetTicketReturnsPathHATEOASLink(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/ticket/ticket-existing", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if body := rec.Body.String(); !containsAll(
		body,
		`"name":"ticket-existing"`,
		`"path":{"href":"/api/v1/path?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"GET"}`,
	) {
		t.Fatalf("unexpected response body: %q", body)
	}
}

func TestPatchTicketUpdatesLimits(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPatch, "/api/v1/ticket/ticket-existing", strings.NewReader(`{"maximum_uses":0,"lifetime_minutes":0}`))
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if body := rec.Body.String(); !containsAll(body, `"name":"ticket-existing"`, `"uses_count":1`) {
		t.Fatalf("unexpected response body: %q", body)
	}
	if strings.Contains(rec.Body.String(), `"expiration_time"`) {
		t.Fatalf("expected expiration_time to be cleared, got %q", rec.Body.String())
	}
}

func TestDeleteTicketRemovesTicket(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/ticket/ticket-existing", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", rec.Code)
	}
}

func TestGetPathContentsSupportsRangeRequests(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/path/contents?irods_path=/tempZone/home/test1/file.txt", nil)
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Range", "bytes=5-10")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusPartialContent {
		t.Fatalf("expected 206, got %d", rec.Code)
	}

	if got := rec.Header().Get("Content-Range"); got == "" {
		t.Fatal("expected Content-Range header")
	}

	if body := rec.Body.String(); body != " conte" {
		t.Fatalf("unexpected ranged content body %q", body)
	}
}

func TestGetPathContentsSupportsSuffixRangeRequests(t *testing.T) {
	handler, filesystem := testHandlerWithConfig(t, nil)
	filePath := "/tempZone/home/test1/file.txt"
	fileContent := filesystem.contentByPath[filePath]
	filesystem.entriesByPath[filePath].Size = int64(len(fileContent))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/path/contents?irods_path=/tempZone/home/test1/file.txt", nil)
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Range", "bytes=-7")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusPartialContent {
		t.Fatalf("expected 206, got %d", rec.Code)
	}
	if got := rec.Header().Get("Content-Range"); got == "" {
		t.Fatal("expected Content-Range header")
	}
	if body := rec.Body.String(); body != "payload" {
		t.Fatalf("unexpected ranged content body %q", body)
	}
}

func TestGetPathContentsIgnoresUnknownRangeUnit(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/path/contents?irods_path=/tempZone/home/test1/file.txt", nil)
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Range", "items=0-3")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if got := rec.Header().Get("Content-Range"); got != "" {
		t.Fatalf("expected no Content-Range header, got %q", got)
	}
	if body := rec.Body.String(); body != "hello content payload" {
		t.Fatalf("unexpected content body %q", body)
	}
}

func TestHeadPathContentsReturnsHeadersOnly(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodHead, "/api/v1/path/contents?irods_path=/tempZone/home/test1/file.txt", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	if rec.Body.Len() != 0 {
		t.Fatalf("expected empty body for HEAD, got %q", rec.Body.String())
	}

	if got := rec.Header().Get("Content-Length"); got == "" {
		t.Fatal("expected Content-Length header")
	}
	if got := rec.Header().Get("Content-Disposition"); !containsAll(got, `attachment`, `filename="file.txt"`) {
		t.Fatalf("expected Content-Disposition header, got %q", got)
	}
	if got := rec.Header().Get("ETag"); got != `"sha2:YWJjMTIz"` {
		t.Fatalf("expected ETag header, got %q", got)
	}
}

func TestHeadPathContentsIgnoresRangeHeader(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodHead, "/api/v1/path/contents?irods_path=/tempZone/home/test1/file.txt", nil)
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Range", "bytes=1-4")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("expected empty body for HEAD, got %q", rec.Body.String())
	}
	if got := rec.Header().Get("Content-Range"); got != "" {
		t.Fatalf("expected no Content-Range header for HEAD, got %q", got)
	}
}

func TestGetPathContentsRejectsInvalidRange(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/path/contents?irods_path=/tempZone/home/test1/file.txt", nil)
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Range", "bytes=999-1000")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("expected 416, got %d", rec.Code)
	}
	if got := rec.Header().Get("Content-Range"); got != "bytes */128" {
		t.Fatalf("expected unsatisfied Content-Range header, got %q", got)
	}
	if body := rec.Body.String(); !containsAll(body, `"code":"invalid_range"`) {
		t.Fatalf("unexpected invalid range response body: %q", body)
	}
}

func TestGetPathContentsStreamsLargeObject(t *testing.T) {
	handler, filesystem := testHandlerWithConfig(t, nil)

	largePath := "/tempZone/home/test1/large.bin"
	largePayload := bytes.Repeat([]byte("0123456789abcdef"), 65536) // 1 MiB
	now := time.Unix(1_700_000_005, 0)
	largeEntry := &irodsfs.Entry{
		ID:         5001,
		Type:       irodsfs.FileEntry,
		Name:       "large.bin",
		Owner:      "alice",
		Path:       largePath,
		Size:       int64(len(largePayload)),
		DataType:   "generic",
		CreateTime: now,
		ModifyTime: now,
	}
	filesystem.entriesByPath[largePath] = largeEntry
	filesystem.contentByPath[largePath] = largePayload
	filesystem.childrenByPath["/tempZone/home/test1"] = append(filesystem.childrenByPath["/tempZone/home/test1"], largeEntry)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/path/contents?irods_path=/tempZone/home/test1/large.bin", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if got := rec.Header().Get("Accept-Ranges"); got != "bytes" {
		t.Fatalf("expected Accept-Ranges header, got %q", got)
	}
	if got := rec.Header().Get("Content-Length"); got != strconv.Itoa(len(largePayload)) {
		t.Fatalf("expected Content-Length %d, got %q", len(largePayload), got)
	}

	body := rec.Body.Bytes()
	if len(body) != len(largePayload) {
		t.Fatalf("expected %d bytes, got %d", len(largePayload), len(body))
	}
	if !bytes.Equal(body[:64], largePayload[:64]) {
		t.Fatalf("unexpected prefix bytes in streamed payload")
	}
	if !bytes.Equal(body[len(body)-64:], largePayload[len(largePayload)-64:]) {
		t.Fatalf("unexpected suffix bytes in streamed payload")
	}
}

func TestExtFavoritesLifecycle(t *testing.T) {
	handler := testHandler(t)

	createReq := httptest.NewRequest(http.MethodPost, "/api/v1/ext/favorites", strings.NewReader(`{"name":"Project File","absolute_path":"/tempZone/home/test1/file.txt"}`))
	createReq.Header.Set("Authorization", "Bearer token123")
	createReq.Header.Set("Content-Type", "application/json")
	createRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(createRec, createReq)

	if createRec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", createRec.Code, createRec.Body.String())
	}
	if body := createRec.Body.String(); !containsAll(
		body,
		`"favorite":{"name":"Project File","absolute_path":"/tempZone/home/test1/file.txt"`,
		`"self":{"href":"/api/v1/ext/favorites?absolute_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"GET"}`,
		`"details":{"href":"/api/v1/path?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"GET"}`,
		`"update":{"href":"/api/v1/ext/favorites","method":"PUT"}`,
		`"delete":{"href":"/api/v1/ext/favorites","method":"DELETE"}`,
	) {
		t.Fatalf("unexpected create favorite body: %q", body)
	}

	listReq := httptest.NewRequest(http.MethodGet, "/api/v1/ext/favorites", nil)
	listReq.Header.Set("Authorization", "Bearer token123")
	listRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", listRec.Code, listRec.Body.String())
	}
	if body := listRec.Body.String(); !containsAll(
		body,
		`"favorites":[`,
		`"name":"Project File"`,
		`"absolute_path":"/tempZone/home/test1/file.txt"`,
		`"count":1`,
		`"links":{"self":{"href":"/api/v1/ext/favorites","method":"GET"}`,
		`"create":{"href":"/api/v1/ext/favorites","method":"POST"}`,
		`"update":{"href":"/api/v1/ext/favorites","method":"PUT"}`,
		`"delete":{"href":"/api/v1/ext/favorites","method":"DELETE"}`,
	) {
		t.Fatalf("unexpected list favorites body: %q", body)
	}

	renameReq := httptest.NewRequest(http.MethodPut, "/api/v1/ext/favorites", strings.NewReader(`{"name":"Renamed Favorite","absolute_path":"/tempZone/home/test1/file.txt"}`))
	renameReq.Header.Set("Authorization", "Bearer token123")
	renameReq.Header.Set("Content-Type", "application/json")
	renameRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(renameRec, renameReq)
	if renameRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", renameRec.Code, renameRec.Body.String())
	}
	if body := renameRec.Body.String(); !containsAll(body, `"favorite":{"name":"Renamed Favorite","absolute_path":"/tempZone/home/test1/file.txt"`) {
		t.Fatalf("unexpected rename favorite body: %q", body)
	}

	filterReq := httptest.NewRequest(http.MethodGet, "/api/v1/ext/favorites?absolute_path=/tempZone/home/test1/file.txt", nil)
	filterReq.Header.Set("Authorization", "Bearer token123")
	filterRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(filterRec, filterReq)
	if filterRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", filterRec.Code, filterRec.Body.String())
	}
	if body := filterRec.Body.String(); !containsAll(body, `"count":1`, `"name":"Renamed Favorite"`) {
		t.Fatalf("unexpected filtered favorite body: %q", body)
	}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/api/v1/ext/favorites", strings.NewReader(`{"absolute_path":"/tempZone/home/test1/file.txt"}`))
	deleteReq.Header.Set("Authorization", "Bearer token123")
	deleteReq.Header.Set("Content-Type", "application/json")
	deleteRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(deleteRec, deleteReq)
	if deleteRec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d: %s", deleteRec.Code, deleteRec.Body.String())
	}

	listAfterDeleteReq := httptest.NewRequest(http.MethodGet, "/api/v1/ext/favorites", nil)
	listAfterDeleteReq.Header.Set("Authorization", "Bearer token123")
	listAfterDeleteRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(listAfterDeleteRec, listAfterDeleteReq)
	if listAfterDeleteRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", listAfterDeleteRec.Code, listAfterDeleteRec.Body.String())
	}
	if body := listAfterDeleteRec.Body.String(); !containsAll(body, `"favorites":[]`, `"count":0`) {
		t.Fatalf("unexpected favorites after delete body: %q", body)
	}
}

func TestExtFavoritesValidationAndNotFound(t *testing.T) {
	handler := testHandler(t)

	invalidCreateReq := httptest.NewRequest(http.MethodPost, "/api/v1/ext/favorites", strings.NewReader(`{"name":"","absolute_path":"relative/path"}`))
	invalidCreateReq.Header.Set("Authorization", "Bearer token123")
	invalidCreateReq.Header.Set("Content-Type", "application/json")
	invalidCreateRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(invalidCreateRec, invalidCreateReq)
	if invalidCreateRec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", invalidCreateRec.Code, invalidCreateRec.Body.String())
	}
	if body := invalidCreateRec.Body.String(); !containsAll(body, `"name":"name is required"`, `"absolute_path":"absolute_path must be an absolute iRODS path"`) {
		t.Fatalf("unexpected invalid create favorite body: %q", body)
	}

	renameMissingReq := httptest.NewRequest(http.MethodPut, "/api/v1/ext/favorites", strings.NewReader(`{"name":"new","absolute_path":"/tempZone/home/test1/missing.txt"}`))
	renameMissingReq.Header.Set("Authorization", "Bearer token123")
	renameMissingReq.Header.Set("Content-Type", "application/json")
	renameMissingRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(renameMissingRec, renameMissingReq)
	if renameMissingRec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", renameMissingRec.Code, renameMissingRec.Body.String())
	}

	invalidFilterReq := httptest.NewRequest(http.MethodGet, "/api/v1/ext/favorites?absolute_path=relative/path", nil)
	invalidFilterReq.Header.Set("Authorization", "Bearer token123")
	invalidFilterRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(invalidFilterRec, invalidFilterReq)
	if invalidFilterRec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", invalidFilterRec.Code, invalidFilterRec.Body.String())
	}
}

func TestExtMetadataQueriesLifecycle(t *testing.T) {
	handler := testHandler(t)

	createBody := `{
		"name": "Frog AVU query",
		"description": "find frog-tagged entries",
		"query": {
			"type": "entry_query",
			"kinds": ["data_object", "collection"],
			"scope": {
				"root": "/tempZone/home/test1/project",
				"mode": "descendants"
			},
			"conditions": [
				{"field": "avu.attrib", "op": "=", "value": "source"},
				{"field": "avu.value", "op": "=", "value": "test"}
			],
			"defaults": {
				"limit": 25,
				"include_matched_avus": true
			}
		}
	}`
	createReq := httptest.NewRequest(http.MethodPost, "/api/v1/ext/metadata-queries", strings.NewReader(createBody))
	createReq.Header.Set("Authorization", "Bearer token123")
	createReq.Header.Set("Content-Type", "application/json")
	createRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(createRec, createReq)

	if createRec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", createRec.Code, createRec.Body.String())
	}

	var createResponse struct {
		MetadataQuery struct {
			metadataext.SavedEntryQuery
			Links map[string]domain.ActionLink `json:"links"`
		} `json:"metadata_query"`
	}
	if err := json.Unmarshal(createRec.Body.Bytes(), &createResponse); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	queryID := createResponse.MetadataQuery.ID
	if queryID == "" {
		t.Fatalf("expected created metadata query id: %q", createRec.Body.String())
	}
	if createResponse.MetadataQuery.Name != "Frog AVU query" {
		t.Fatalf("unexpected created metadata query: %+v", createResponse.MetadataQuery)
	}
	if createResponse.MetadataQuery.Query.Type != metadataext.EntryQueryDefinitionType {
		t.Fatalf("expected canonical entry query type, got %q", createResponse.MetadataQuery.Query.Type)
	}
	if len(createResponse.MetadataQuery.Query.Conditions) == 0 {
		t.Fatalf("expected canonical query conditions: %+v", createResponse.MetadataQuery.Query)
	}
	if _, ok := createResponse.MetadataQuery.Links["delete"]; !ok {
		t.Fatalf("expected delete link in create response: %q", createRec.Body.String())
	}

	listReq := httptest.NewRequest(http.MethodGet, "/api/v1/ext/metadata-queries", nil)
	listReq.Header.Set("Authorization", "Bearer token123")
	listRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", listRec.Code, listRec.Body.String())
	}
	if body := listRec.Body.String(); !containsAll(
		body,
		`"metadata_queries":[`,
		`"name":"Frog AVU query"`,
		`"file_name":"`+queryID+`.entry-query.json"`,
		`"count":1`,
		`"create":{"href":"/api/v1/ext/metadata-queries","method":"POST"}`,
	) {
		t.Fatalf("unexpected list metadata queries body: %q", body)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/ext/metadata-queries/"+queryID, nil)
	getReq.Header.Set("Authorization", "Bearer token123")
	getRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", getRec.Code, getRec.Body.String())
	}
	if body := getRec.Body.String(); !containsAll(body, `"id":"`+queryID+`"`, `"name":"Frog AVU query"`) {
		t.Fatalf("unexpected get metadata query body: %q", body)
	}

	updateBody := `{
		"name": "Updated Frog AVU query",
		"description": "updated",
		"query": {
			"kinds": ["data_object"],
			"conditions": [
				{"field": "avu.attrib", "op": "=", "value": "source"},
				{"field": "avu.value", "op": "like", "value": "te%"}
			],
			"defaults": {
				"limit": 10
			}
		}
	}`
	updateReq := httptest.NewRequest(http.MethodPut, "/api/v1/ext/metadata-queries/"+queryID, strings.NewReader(updateBody))
	updateReq.Header.Set("Authorization", "Bearer token123")
	updateReq.Header.Set("Content-Type", "application/json")
	updateRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(updateRec, updateReq)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", updateRec.Code, updateRec.Body.String())
	}
	if body := updateRec.Body.String(); !containsAll(body, `"id":"`+queryID+`"`, `"name":"Updated Frog AVU query"`, `"description":"updated"`, `"limit":10`) {
		t.Fatalf("unexpected update metadata query body: %q", body)
	}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/api/v1/ext/metadata-queries/"+queryID, nil)
	deleteReq.Header.Set("Authorization", "Bearer token123")
	deleteRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(deleteRec, deleteReq)
	if deleteRec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d: %s", deleteRec.Code, deleteRec.Body.String())
	}

	getDeletedReq := httptest.NewRequest(http.MethodGet, "/api/v1/ext/metadata-queries/"+queryID, nil)
	getDeletedReq.Header.Set("Authorization", "Bearer token123")
	getDeletedRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(getDeletedRec, getDeletedReq)
	if getDeletedRec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", getDeletedRec.Code, getDeletedRec.Body.String())
	}
}

func TestExtMetadataQueriesDisplayDefaultsAndDuplicateNames(t *testing.T) {
	handler := testHandler(t)

	createBody := `{
		"query": {
			"kinds": ["data_object"],
			"conditions": [
				{"field": "avu.attrib", "op": "=", "value": "source"},
				{"field": "avu.value", "op": "like", "value": "te%"}
			]
		}
	}`
	createReq := httptest.NewRequest(http.MethodPost, "/api/v1/ext/metadata-queries", strings.NewReader(createBody))
	createReq.Header.Set("Authorization", "Bearer token123")
	createReq.Header.Set("Content-Type", "application/json")
	createRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", createRec.Code, createRec.Body.String())
	}

	var createResponse struct {
		MetadataQuery metadataext.SavedEntryQuery `json:"metadata_query"`
	}
	if err := json.Unmarshal(createRec.Body.Bytes(), &createResponse); err != nil {
		t.Fatalf("decode default-name create response: %v", err)
	}
	firstID := createResponse.MetadataQuery.ID
	if strings.TrimSpace(firstID) == "" {
		t.Fatalf("expected created saved query id: %q", createRec.Body.String())
	}
	if createResponse.MetadataQuery.Name != defaultSavedMetadataQueryName {
		t.Fatalf("expected default saved query name %q, got %q", defaultSavedMetadataQueryName, createResponse.MetadataQuery.Name)
	}
	if createResponse.MetadataQuery.Description != "" {
		t.Fatalf("expected blank default description, got %q", createResponse.MetadataQuery.Description)
	}

	updateBody := `{
		"name": "   ",
		"description": "   ",
		"query": {
			"kinds": ["collection"],
			"conditions": [
				{"field": "avu.attrib", "op": "=", "value": "source"}
			]
		}
	}`
	updateReq := httptest.NewRequest(http.MethodPut, "/api/v1/ext/metadata-queries/"+firstID, strings.NewReader(updateBody))
	updateReq.Header.Set("Authorization", "Bearer token123")
	updateReq.Header.Set("Content-Type", "application/json")
	updateRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(updateRec, updateReq)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", updateRec.Code, updateRec.Body.String())
	}

	var updateResponse struct {
		MetadataQuery metadataext.SavedEntryQuery `json:"metadata_query"`
	}
	if err := json.Unmarshal(updateRec.Body.Bytes(), &updateResponse); err != nil {
		t.Fatalf("decode default-name update response: %v", err)
	}
	if updateResponse.MetadataQuery.ID != firstID {
		t.Fatalf("expected update to preserve route id %q, got %q", firstID, updateResponse.MetadataQuery.ID)
	}
	if updateResponse.MetadataQuery.Name != defaultSavedMetadataQueryName {
		t.Fatalf("expected blank update name to default to %q, got %q", defaultSavedMetadataQueryName, updateResponse.MetadataQuery.Name)
	}
	if updateResponse.MetadataQuery.Description != "" {
		t.Fatalf("expected blank update description, got %q", updateResponse.MetadataQuery.Description)
	}

	duplicateReq := httptest.NewRequest(http.MethodPost, "/api/v1/ext/metadata-queries", strings.NewReader(`{
		"name": "New Query",
		"description": "duplicate display name",
		"query": {
			"kinds": ["data_object"],
			"conditions": [
				{"field": "avu.attrib", "op": "=", "value": "source"}
			]
		}
	}`))
	duplicateReq.Header.Set("Authorization", "Bearer token123")
	duplicateReq.Header.Set("Content-Type", "application/json")
	duplicateRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(duplicateRec, duplicateReq)
	if duplicateRec.Code != http.StatusCreated {
		t.Fatalf("expected 201 for duplicate display name, got %d: %s", duplicateRec.Code, duplicateRec.Body.String())
	}

	var duplicateResponse struct {
		MetadataQuery metadataext.SavedEntryQuery `json:"metadata_query"`
	}
	if err := json.Unmarshal(duplicateRec.Body.Bytes(), &duplicateResponse); err != nil {
		t.Fatalf("decode duplicate-name create response: %v", err)
	}
	if duplicateResponse.MetadataQuery.ID == "" || duplicateResponse.MetadataQuery.ID == firstID {
		t.Fatalf("expected duplicate display name to create distinct id, first=%q duplicate=%q", firstID, duplicateResponse.MetadataQuery.ID)
	}
	if duplicateResponse.MetadataQuery.Name != defaultSavedMetadataQueryName {
		t.Fatalf("expected duplicate display name %q, got %q", defaultSavedMetadataQueryName, duplicateResponse.MetadataQuery.Name)
	}

	listReq := httptest.NewRequest(http.MethodGet, "/api/v1/ext/metadata-queries", nil)
	listReq.Header.Set("Authorization", "Bearer token123")
	listRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", listRec.Code, listRec.Body.String())
	}
	if body := listRec.Body.String(); !containsAll(body, `"id":"`+firstID+`"`, `"id":"`+duplicateResponse.MetadataQuery.ID+`"`, `"name":"New Query"`, `"count":2`) {
		t.Fatalf("unexpected duplicate-name list body: %q", body)
	}
}

func TestExtMetadataQueriesValidation(t *testing.T) {
	handler := testHandler(t)

	missingFieldsReq := httptest.NewRequest(http.MethodPost, "/api/v1/ext/metadata-queries", strings.NewReader(`{"name":""}`))
	missingFieldsReq.Header.Set("Authorization", "Bearer token123")
	missingFieldsReq.Header.Set("Content-Type", "application/json")
	missingFieldsRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(missingFieldsRec, missingFieldsReq)
	if missingFieldsRec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", missingFieldsRec.Code, missingFieldsRec.Body.String())
	}
	if body := missingFieldsRec.Body.String(); !containsAll(body, `"query":"query is required"`) || strings.Contains(body, `"name":"name is required"`) {
		t.Fatalf("unexpected missing fields body: %q", body)
	}

	invalidQueryReq := httptest.NewRequest(http.MethodPost, "/api/v1/ext/metadata-queries", strings.NewReader(`{
		"name": "bad query",
		"query": {
			"type": "entry_query",
			"replica_policy": "all"
		}
	}`))
	invalidQueryReq.Header.Set("Authorization", "Bearer token123")
	invalidQueryReq.Header.Set("Content-Type", "application/json")
	invalidQueryRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(invalidQueryRec, invalidQueryReq)
	if invalidQueryRec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", invalidQueryRec.Code, invalidQueryRec.Body.String())
	}
}

func TestExtS3BucketsLifecycle(t *testing.T) {
	mappingPath := path.Join(t.TempDir(), "bucket-mapping.json")
	handler, _ := testHandlerWithConfig(t, func(cfg *config.RestConfig) {
		cfg.S3ApiSupported = true
		cfg.S3BucketMappingFile = mappingPath
	})

	createReq := httptest.NewRequest(http.MethodPost, "/api/v1/ext/s3/buckets", strings.NewReader(`{"bucket_name":"project-bucket","irods_path":"/tempZone/home/test1/project"}`))
	createReq.Header.Set("Authorization", "Bearer token123")
	createReq.Header.Set("Content-Type", "application/json")
	createRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", createRec.Code, createRec.Body.String())
	}
	if body := createRec.Body.String(); !containsAll(body, `"bucket_id":"project-bucket"`, `"irods_path":"/tempZone/home/test1/project"`, `"self":{"href":"/api/v1/ext/s3/buckets/project-bucket","method":"GET"}`) {
		t.Fatalf("unexpected create bucket body: %q", body)
	}

	autoCreateReq := httptest.NewRequest(http.MethodPost, "/api/v1/ext/s3/buckets", strings.NewReader(`{"auto_generate":true,"irods_path":"/tempZone/home/test1/project/nested"}`))
	autoCreateReq.Header.Set("Authorization", "Bearer token123")
	autoCreateReq.Header.Set("Content-Type", "application/json")
	autoCreateRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(autoCreateRec, autoCreateReq)
	if autoCreateRec.Code != http.StatusCreated {
		t.Fatalf("expected 201 auto-creating bucket, got %d: %s", autoCreateRec.Code, autoCreateRec.Body.String())
	}

	listReq := httptest.NewRequest(http.MethodGet, "/api/v1/ext/s3/buckets?irods_path=/tempZone/home/test1/project&recursive=true", nil)
	listReq.Header.Set("Authorization", "Bearer token123")
	listRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("expected 200 listing buckets, got %d: %s", listRec.Code, listRec.Body.String())
	}
	if body := listRec.Body.String(); !containsAll(body, `"buckets":[`, `"bucket_id":"project-bucket"`, `"irods_path":"/tempZone/home/test1/project"`, `"irods_path":"/tempZone/home/test1/project/nested"`, `"count":2`) {
		t.Fatalf("unexpected list buckets body: %q", body)
	}

	getByIDReq := httptest.NewRequest(http.MethodGet, "/api/v1/ext/s3/buckets/project-bucket?irods_path=/tempZone/home/test1/project&recursive=true", nil)
	getByIDReq.Header.Set("Authorization", "Bearer token123")
	getByIDRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(getByIDRec, getByIDReq)
	if getByIDRec.Code != http.StatusOK {
		t.Fatalf("expected 200 getting bucket by id, got %d: %s", getByIDRec.Code, getByIDRec.Body.String())
	}
	if body := getByIDRec.Body.String(); !containsAll(body, `"bucket_id":"project-bucket"`, `"irods_path":"/tempZone/home/test1/project"`) {
		t.Fatalf("unexpected get bucket by id body: %q", body)
	}

	getByPathReq := httptest.NewRequest(http.MethodGet, "/api/v1/ext/s3/buckets/by-path?irods_path=/tempZone/home/test1/project", nil)
	getByPathReq.Header.Set("Authorization", "Bearer token123")
	getByPathRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(getByPathRec, getByPathReq)
	if getByPathRec.Code != http.StatusOK {
		t.Fatalf("expected 200 getting bucket by path, got %d: %s", getByPathRec.Code, getByPathRec.Body.String())
	}
	if body := getByPathRec.Body.String(); !containsAll(body, `"bucket_id":"project-bucket"`, `"irods_path":"/tempZone/home/test1/project"`) {
		t.Fatalf("unexpected get bucket by path body: %q", body)
	}

	updateReq := httptest.NewRequest(http.MethodPut, "/api/v1/ext/s3/buckets", strings.NewReader(`{"bucket_name":"project-renamed","irods_path":"/tempZone/home/test1/project"}`))
	updateReq.Header.Set("Authorization", "Bearer token123")
	updateReq.Header.Set("Content-Type", "application/json")
	updateRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(updateRec, updateReq)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("expected 200 updating bucket, got %d: %s", updateRec.Code, updateRec.Body.String())
	}
	if body := updateRec.Body.String(); !containsAll(body, `"bucket_id":"project-renamed"`, `"irods_path":"/tempZone/home/test1/project"`) {
		t.Fatalf("unexpected update bucket body: %q", body)
	}

	duplicateReq := httptest.NewRequest(http.MethodPost, "/api/v1/ext/s3/buckets", strings.NewReader(`{"bucket_name":"project-renamed","irods_path":"/tempZone/home/test1/project/nested"}`))
	duplicateReq.Header.Set("Authorization", "Bearer token123")
	duplicateReq.Header.Set("Content-Type", "application/json")
	duplicateRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(duplicateRec, duplicateReq)
	if duplicateRec.Code != http.StatusConflict {
		t.Fatalf("expected 409 for duplicate bucket, got %d: %s", duplicateRec.Code, duplicateRec.Body.String())
	}

	autoBucketID := assertS3BucketMappingPath(t, mappingPath, "/tempZone/home/test1/project/nested")
	if err := os.WriteFile(mappingPath, []byte(`{"stale-bucket":"/tempZone/home/test1/stale"}`), 0o644); err != nil {
		t.Fatalf("write stale S3 bucket mapping file: %v", err)
	}

	nonAdminRefreshReq := httptest.NewRequest(http.MethodPost, "/api/v1/ext/s3/buckets/refresh-mapping", nil)
	nonAdminRefreshReq.Header.Set("Authorization", "Bearer token123")
	nonAdminRefreshRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(nonAdminRefreshRec, nonAdminRefreshReq)
	if nonAdminRefreshRec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 refreshing bucket mapping as non-admin, got %d: %s", nonAdminRefreshRec.Code, nonAdminRefreshRec.Body.String())
	}
	if body := nonAdminRefreshRec.Body.String(); !containsAll(body, `"code":"permission_denied"`, `"message":"permission denied"`) {
		t.Fatalf("expected permission denied response, got %s", body)
	}

	refreshReq := httptest.NewRequest(http.MethodPost, "/api/v1/ext/s3/buckets/refresh-mapping", nil)
	refreshReq.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("otheradmin:secret")))
	refreshRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(refreshRec, refreshReq)
	if refreshRec.Code != http.StatusOK {
		t.Fatalf("expected 200 refreshing bucket mapping, got %d: %s", refreshRec.Code, refreshRec.Body.String())
	}
	if body := refreshRec.Body.String(); !containsAll(body, `"bucket_mapping":`, `"mapping_file_path":`, `"bucket_id":"project-renamed"`, `"bucket_id":"`+autoBucketID+`"`, `"count":2`) {
		t.Fatalf("unexpected refresh body: %s", body)
	}
	assertS3BucketMappingFile(t, mappingPath, "project-renamed", "/tempZone/home/test1/project")
	assertS3BucketMappingFile(t, mappingPath, autoBucketID, "/tempZone/home/test1/project/nested")
	if mapping := readS3BucketMappingFile(t, mappingPath); mapping["stale-bucket"] != "" {
		t.Fatalf("expected stale mapping to be removed after refresh, got %+v", mapping)
	}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/api/v1/ext/s3/buckets/project-renamed", nil)
	deleteReq.Header.Set("Authorization", "Bearer token123")
	deleteRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(deleteRec, deleteReq)
	if deleteRec.Code != http.StatusNoContent {
		t.Fatalf("expected 204 deleting bucket, got %d: %s", deleteRec.Code, deleteRec.Body.String())
	}

	mapping := readS3BucketMappingFile(t, mappingPath)
	if _, ok := mapping["project-renamed"]; ok {
		t.Fatalf("expected deleted bucket to be removed from mapping, got %+v", mapping)
	}
}

func TestExtS3BucketsValidationAndConfiguration(t *testing.T) {
	unsupportedHandler, _ := testHandlerWithConfig(t, func(cfg *config.RestConfig) {
		cfg.S3ApiSupported = false
	})

	unsupportedReq := httptest.NewRequest(http.MethodGet, "/api/v1/ext/s3/buckets", nil)
	unsupportedReq.Header.Set("Authorization", "Bearer token123")
	unsupportedRec := httptest.NewRecorder()
	unsupportedHandler.Routes().ServeHTTP(unsupportedRec, unsupportedReq)
	if unsupportedRec.Code != http.StatusNotImplemented {
		t.Fatalf("expected 501 for unsupported S3 API, got %d: %s", unsupportedRec.Code, unsupportedRec.Body.String())
	}

	handler, _ := testHandlerWithConfig(t, func(cfg *config.RestConfig) {
		cfg.S3ApiSupported = true
		cfg.S3BucketMappingFile = ""
	})

	createReq := httptest.NewRequest(http.MethodPost, "/api/v1/ext/s3/buckets", strings.NewReader(`{"bucket_name":"","irods_path":"relative/path"}`))
	createReq.Header.Set("Authorization", "Bearer token123")
	createReq.Header.Set("Content-Type", "application/json")
	createRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", createRec.Code, createRec.Body.String())
	}
	if body := createRec.Body.String(); !containsAll(body, `"bucket_name":"bucket_name is required unless auto_generate is true"`, `"irods_path":"irods_path must be an absolute iRODS path"`) {
		t.Fatalf("unexpected validation body: %q", body)
	}

	bucketIDOnlyReq := httptest.NewRequest(http.MethodPost, "/api/v1/ext/s3/buckets", strings.NewReader(`{"bucket_id":"legacy-bucket","irods_path":"/tempZone/home/test1/project"}`))
	bucketIDOnlyReq.Header.Set("Authorization", "Bearer token123")
	bucketIDOnlyReq.Header.Set("Content-Type", "application/json")
	bucketIDOnlyRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(bucketIDOnlyRec, bucketIDOnlyReq)
	if bucketIDOnlyRec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for bucket_id-only S3 bucket request, got %d: %s", bucketIDOnlyRec.Code, bucketIDOnlyRec.Body.String())
	}
	if body := bucketIDOnlyRec.Body.String(); !containsAll(body, `"bucket_name":"bucket_name is required unless auto_generate is true"`) {
		t.Fatalf("expected bucket_name validation field in response, got %s", body)
	}

	listReq := httptest.NewRequest(http.MethodGet, "/api/v1/ext/s3/buckets?irods_path=/tempZone/home/test1/project", nil)
	listReq.Header.Set("Authorization", "Bearer token123")
	listRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 for missing S3BucketMappingFile, got %d: %s", listRec.Code, listRec.Body.String())
	}
}

func TestExtS3UserSecretsLifecycle(t *testing.T) {
	dir := t.TempDir()
	bucketMappingPath := path.Join(dir, "bucket-mapping.json")
	userMappingPath := path.Join(dir, "user-mapping.json")
	handler, _ := testHandlerWithConfig(t, func(cfg *config.RestConfig) {
		cfg.S3ApiSupported = true
		cfg.S3BucketMappingFile = bucketMappingPath
		cfg.S3UserMappingFile = userMappingPath
	})

	initialSecret := "Aa1Bb~2Cc3.-Dd4Ee5Ff6Gg7Hh8Ii9_Jj0Kk1Ll2"
	createReq := httptest.NewRequest(http.MethodPost, "/api/v1/ext/s3/user-secrets", strings.NewReader(`{"user_name":"alice","secret_key":"`+initialSecret+`"}`))
	createReq.Header.Set("Authorization", "Bearer token123")
	createReq.Header.Set("Content-Type", "application/json")
	createRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("expected 201 creating S3 user secret, got %d: %s", createRec.Code, createRec.Body.String())
	}
	if body := createRec.Body.String(); !containsAll(body, `"user_name":"alice"`, `"secret_key":"`+initialSecret+`"`, `"irods_path":"/tempZone/home/alice/.irodsext/s3admin/irods-s3-api-secret.txt"`) {
		t.Fatalf("unexpected create user secret body: %q", body)
	}
	assertS3UserMappingFile(t, userMappingPath, "alice", initialSecret)

	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/ext/s3/user-secrets/alice", nil)
	getReq.Header.Set("Authorization", "Bearer token123")
	getRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("expected 200 getting S3 user secret, got %d: %s", getRec.Code, getRec.Body.String())
	}
	if body := getRec.Body.String(); !containsAll(body, `"user_name":"alice"`, `"secret_key":"`+initialSecret+`"`, `"self":{"href":"/api/v1/ext/s3/user-secrets/alice","method":"GET"}`) {
		t.Fatalf("unexpected get user secret body: %q", body)
	}

	updatedSecret := strings.Repeat("Z", s3adminext.S3UserSecretKeyLength)
	updateReq := httptest.NewRequest(http.MethodPut, "/api/v1/ext/s3/user-secrets", strings.NewReader(`{"user_name":"alice","secret_key":"`+updatedSecret+`"}`))
	updateReq.Header.Set("Authorization", "Bearer token123")
	updateReq.Header.Set("Content-Type", "application/json")
	updateRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(updateRec, updateReq)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("expected 200 updating S3 user secret, got %d: %s", updateRec.Code, updateRec.Body.String())
	}
	assertS3UserMappingFile(t, userMappingPath, "alice", updatedSecret)

	generateReq := httptest.NewRequest(http.MethodPut, "/api/v1/ext/s3/user-secrets", strings.NewReader(`{"user_name":"alice","auto_generate":true}`))
	generateReq.Header.Set("Authorization", "Bearer token123")
	generateReq.Header.Set("Content-Type", "application/json")
	generateRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(generateRec, generateReq)
	if generateRec.Code != http.StatusOK {
		t.Fatalf("expected 200 generating S3 user secret, got %d: %s", generateRec.Code, generateRec.Body.String())
	}
	generatedSecret := readS3UserMappingFile(t, userMappingPath)["alice"].SecretKey
	if generatedSecret == "" || generatedSecret == updatedSecret {
		t.Fatalf("expected generated secret to replace previous mapping, got %q", generatedSecret)
	}

	nonAdminCrossGetReq := httptest.NewRequest(http.MethodGet, "/api/v1/ext/s3/user-secrets/test1", nil)
	nonAdminCrossGetReq.Header.Set("Authorization", "Bearer token123")
	nonAdminCrossGetRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(nonAdminCrossGetRec, nonAdminCrossGetReq)
	if nonAdminCrossGetRec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 getting other user's secret as non-admin, got %d: %s", nonAdminCrossGetRec.Code, nonAdminCrossGetRec.Body.String())
	}

	nonAdminCrossCreateReq := httptest.NewRequest(http.MethodPost, "/api/v1/ext/s3/user-secrets", strings.NewReader(`{"user_name":"test1","secret_key":"`+initialSecret+`"}`))
	nonAdminCrossCreateReq.Header.Set("Authorization", "Bearer token123")
	nonAdminCrossCreateReq.Header.Set("Content-Type", "application/json")
	nonAdminCrossCreateRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(nonAdminCrossCreateRec, nonAdminCrossCreateReq)
	if nonAdminCrossCreateRec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 creating other user's secret as non-admin, got %d: %s", nonAdminCrossCreateRec.Code, nonAdminCrossCreateRec.Body.String())
	}

	nonAdminCrossUpdateReq := httptest.NewRequest(http.MethodPut, "/api/v1/ext/s3/user-secrets", strings.NewReader(`{"user_name":"test1","secret_key":"`+updatedSecret+`"}`))
	nonAdminCrossUpdateReq.Header.Set("Authorization", "Bearer token123")
	nonAdminCrossUpdateReq.Header.Set("Content-Type", "application/json")
	nonAdminCrossUpdateRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(nonAdminCrossUpdateRec, nonAdminCrossUpdateReq)
	if nonAdminCrossUpdateRec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 updating other user's secret as non-admin, got %d: %s", nonAdminCrossUpdateRec.Code, nonAdminCrossUpdateRec.Body.String())
	}

	nonAdminCrossDeleteReq := httptest.NewRequest(http.MethodDelete, "/api/v1/ext/s3/user-secrets/test1", nil)
	nonAdminCrossDeleteReq.Header.Set("Authorization", "Bearer token123")
	nonAdminCrossDeleteRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(nonAdminCrossDeleteRec, nonAdminCrossDeleteReq)
	if nonAdminCrossDeleteRec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 deleting other user's secret as non-admin, got %d: %s", nonAdminCrossDeleteRec.Code, nonAdminCrossDeleteRec.Body.String())
	}

	adminCrossCreateReq := httptest.NewRequest(http.MethodPost, "/api/v1/ext/s3/user-secrets", strings.NewReader(`{"user_name":"test1","secret_key":"`+initialSecret+`"}`))
	adminCrossCreateReq.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("otheradmin:secret")))
	adminCrossCreateReq.Header.Set("Content-Type", "application/json")
	adminCrossCreateRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(adminCrossCreateRec, adminCrossCreateReq)
	if adminCrossCreateRec.Code != http.StatusCreated {
		t.Fatalf("expected 201 creating other user's secret as rodsadmin, got %d: %s", adminCrossCreateRec.Code, adminCrossCreateRec.Body.String())
	}

	adminCrossDeleteReq := httptest.NewRequest(http.MethodDelete, "/api/v1/ext/s3/user-secrets/test1", nil)
	adminCrossDeleteReq.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("otheradmin:secret")))
	adminCrossDeleteRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(adminCrossDeleteRec, adminCrossDeleteReq)
	if adminCrossDeleteRec.Code != http.StatusNoContent {
		t.Fatalf("expected 204 deleting other user's secret as rodsadmin, got %d: %s", adminCrossDeleteRec.Code, adminCrossDeleteRec.Body.String())
	}

	nonAdminListReq := httptest.NewRequest(http.MethodGet, "/api/v1/ext/s3/user-secrets", nil)
	nonAdminListReq.Header.Set("Authorization", "Bearer token123")
	nonAdminListRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(nonAdminListRec, nonAdminListReq)
	if nonAdminListRec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 listing user secrets as non-admin, got %d: %s", nonAdminListRec.Code, nonAdminListRec.Body.String())
	}

	adminListReq := httptest.NewRequest(http.MethodGet, "/api/v1/ext/s3/user-secrets", nil)
	adminListReq.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("otheradmin:secret")))
	adminListRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(adminListRec, adminListReq)
	if adminListRec.Code != http.StatusOK {
		t.Fatalf("expected 200 listing user secrets as rodsadmin, got %d: %s", adminListRec.Code, adminListRec.Body.String())
	}
	if body := adminListRec.Body.String(); !containsAll(body, `"user_secrets":`, `"user_name":"alice"`, `"secret_key":"`+generatedSecret+`"`, `"count":1`) {
		t.Fatalf("unexpected user secret list body: %s", body)
	}

	if err := os.WriteFile(userMappingPath, []byte(`{"stale":{"secret_key":"stale","username":"stale"}}`), 0o644); err != nil {
		t.Fatalf("write stale S3 user mapping file: %v", err)
	}

	nonAdminRefreshReq := httptest.NewRequest(http.MethodPost, "/api/v1/ext/s3/user-secrets/refresh-mapping", nil)
	nonAdminRefreshReq.Header.Set("Authorization", "Bearer token123")
	nonAdminRefreshRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(nonAdminRefreshRec, nonAdminRefreshReq)
	if nonAdminRefreshRec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 refreshing user mapping as non-admin, got %d: %s", nonAdminRefreshRec.Code, nonAdminRefreshRec.Body.String())
	}
	if body := nonAdminRefreshRec.Body.String(); !containsAll(body, `"code":"permission_denied"`, `"message":"permission denied"`) {
		t.Fatalf("expected permission denied response, got %s", body)
	}

	refreshReq := httptest.NewRequest(http.MethodPost, "/api/v1/ext/s3/user-secrets/refresh-mapping", nil)
	refreshReq.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("otheradmin:secret")))
	refreshRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(refreshRec, refreshReq)
	if refreshRec.Code != http.StatusOK {
		t.Fatalf("expected 200 refreshing S3 user mapping, got %d: %s", refreshRec.Code, refreshRec.Body.String())
	}
	if body := refreshRec.Body.String(); !containsAll(body, `"user_mapping":`, `"user_name":"alice"`, `"count":1`) {
		t.Fatalf("unexpected user mapping refresh body: %s", body)
	}
	assertS3UserMappingFile(t, userMappingPath, "alice", generatedSecret)
	if mapping := readS3UserMappingFile(t, userMappingPath); mapping["stale"].SecretKey != "" {
		t.Fatalf("expected stale user mapping to be removed after refresh, got %+v", mapping)
	}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/api/v1/ext/s3/user-secrets/alice", nil)
	deleteReq.Header.Set("Authorization", "Bearer token123")
	deleteRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(deleteRec, deleteReq)
	if deleteRec.Code != http.StatusNoContent {
		t.Fatalf("expected 204 deleting S3 user secret, got %d: %s", deleteRec.Code, deleteRec.Body.String())
	}
	if mapping := readS3UserMappingFile(t, userMappingPath); mapping["alice"].SecretKey != "" {
		t.Fatalf("expected deleted user secret to be removed from mapping, got %+v", mapping)
	}

	getAfterDeleteReq := httptest.NewRequest(http.MethodGet, "/api/v1/ext/s3/user-secrets/alice", nil)
	getAfterDeleteReq.Header.Set("Authorization", "Bearer token123")
	getAfterDeleteRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(getAfterDeleteRec, getAfterDeleteReq)
	if getAfterDeleteRec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 getting deleted S3 user secret, got %d: %s", getAfterDeleteRec.Code, getAfterDeleteRec.Body.String())
	}
}

func TestExtS3UserSecretsValidationAndConfiguration(t *testing.T) {
	unsupportedHandler, _ := testHandlerWithConfig(t, func(cfg *config.RestConfig) {
		cfg.S3ApiSupported = false
	})

	unsupportedReq := httptest.NewRequest(http.MethodGet, "/api/v1/ext/s3/user-secrets/test1", nil)
	unsupportedReq.Header.Set("Authorization", "Bearer token123")
	unsupportedRec := httptest.NewRecorder()
	unsupportedHandler.Routes().ServeHTTP(unsupportedRec, unsupportedReq)
	if unsupportedRec.Code != http.StatusNotImplemented {
		t.Fatalf("expected 501 for unsupported S3 API user secret endpoint, got %d: %s", unsupportedRec.Code, unsupportedRec.Body.String())
	}

	handler, _ := testHandlerWithConfig(t, func(cfg *config.RestConfig) {
		cfg.S3ApiSupported = true
		cfg.S3UserMappingFile = ""
	})

	invalidReq := httptest.NewRequest(http.MethodPost, "/api/v1/ext/s3/user-secrets", strings.NewReader(`{"user_name":"","secret_key":""}`))
	invalidReq.Header.Set("Authorization", "Bearer token123")
	invalidReq.Header.Set("Content-Type", "application/json")
	invalidRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(invalidRec, invalidReq)
	if invalidRec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 validating S3 user secret, got %d: %s", invalidRec.Code, invalidRec.Body.String())
	}
	if body := invalidRec.Body.String(); !containsAll(body, `"user_name":"user_name is required"`, `"secret_key":"secret_key is required unless auto_generate is true"`) {
		t.Fatalf("unexpected S3 user secret validation body: %q", body)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/ext/s3/user-secrets/test1", nil)
	getReq.Header.Set("Authorization", "Bearer token123")
	getRec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 for missing S3UserMappingFile, got %d: %s", getRec.Code, getRec.Body.String())
	}
}

func assertS3BucketMappingFile(t *testing.T, mappingPath string, bucketID string, expectedPath string) {
	t.Helper()

	mapping := readS3BucketMappingFile(t, mappingPath)
	if mapping[bucketID] != expectedPath {
		t.Fatalf("expected mapping %q -> %q, got %+v", bucketID, expectedPath, mapping)
	}
}

func assertS3BucketMappingPath(t *testing.T, mappingPath string, expectedPath string) string {
	t.Helper()

	mapping := readS3BucketMappingFile(t, mappingPath)
	for bucketID, irodsPath := range mapping {
		if irodsPath == expectedPath {
			return bucketID
		}
	}
	t.Fatalf("expected mapping path %q, got %+v", expectedPath, mapping)
	return ""
}

func readS3BucketMappingFile(t *testing.T, mappingPath string) map[string]string {
	t.Helper()

	content, err := os.ReadFile(mappingPath)
	if err != nil {
		t.Fatalf("read s3 bucket mapping file %q: %v", mappingPath, err)
	}

	mapping := map[string]string{}
	if err := json.Unmarshal(content, &mapping); err != nil {
		t.Fatalf("decode s3 bucket mapping file %q: %v", mappingPath, err)
	}
	return mapping
}

func assertS3UserMappingFile(t *testing.T, mappingPath string, userName string, expectedSecretKey string) {
	t.Helper()

	mapping := readS3UserMappingFile(t, mappingPath)
	entry := mapping[userName]
	if entry.SecretKey != expectedSecretKey || entry.Username != userName {
		t.Fatalf("expected user mapping %q -> %q, got %+v", userName, expectedSecretKey, mapping)
	}
}

func readS3UserMappingFile(t *testing.T, mappingPath string) map[string]s3adminext.UserMappingEntry {
	t.Helper()

	content, err := os.ReadFile(mappingPath)
	if err != nil {
		t.Fatalf("read s3 user mapping file %q: %v", mappingPath, err)
	}

	mapping := map[string]s3adminext.UserMappingEntry{}
	if err := json.Unmarshal(content, &mapping); err != nil {
		t.Fatalf("decode s3 user mapping file %q: %v", mappingPath, err)
	}
	return mapping
}

type stubAuthService struct{}

func (stubAuthService) AuthorizationURL(state string) (string, error) {
	return "http://keycloak.local/auth?state=" + url.QueryEscape(state), nil
}

func (stubAuthService) ExchangeCode(_ context.Context, code string) (auth.Token, error) {
	return auth.Token{
		AccessToken: "token123",
		TokenType:   "Bearer",
		ExpiresIn:   300,
	}, nil
}

func (stubAuthService) NewState() (string, error) {
	return "state123", nil
}

func (stubAuthService) VerifyToken(_ context.Context, accessToken string) (auth.Principal, error) {
	if accessToken != "token123" {
		return auth.Principal{}, auth.ErrUnauthorized
	}

	return auth.Principal{
		Subject:  "user-123",
		Username: "alice",
		Scope:    []string{"openid", "profile"},
		ClientID: "irods-go-rest",
		Audience: []string{"irods-go-rest"},
		Active:   true,
	}, nil
}

func testHandler(t *testing.T) *Handler {
	t.Helper()
	handler, _ := testHandlerWithConfig(t, nil)
	return handler
}

func testHandlerWithConfig(t *testing.T, mutate func(*config.RestConfig)) (*Handler, *testCatalogFileSystem) {
	t.Helper()

	cfg, err := config.ReadRestConfig("rest-config", "yaml", []string{"../config"})
	if err != nil {
		t.Fatalf("read rest config: %v", err)
	}
	if strings.TrimSpace(cfg.IrodsZone) == "" {
		cfg.IrodsZone = "tempZone"
	}
	if strings.TrimSpace(cfg.IrodsHost) == "" {
		cfg.IrodsHost = "irods.local"
	}
	if cfg.IrodsPort <= 0 {
		cfg.IrodsPort = 1247
	}
	cfg.IrodsNegotiationPolicy = "CS_NEG_DONT_CARE"
	cfg.IrodsDefaultResource = "demoResc"
	cfg.ResourceAffinity = []string{"demoResc", "edgeResc"}
	if mutate != nil {
		mutate(cfg)
	}
	filesystem := newTestCatalogFileSystem()
	factory := func(_ *irodstypes.IRODSAccount, _ string) (irods.CatalogFileSystem, error) {
		return filesystem, nil
	}

	return NewHandler(
		*cfg,
		restservice.NewPathService(irods.NewCatalogServiceWithFactory(*cfg, factory)),
		restservice.NewS3AdminService(irods.NewCatalogServiceWithFactory(*cfg, factory)),
		restservice.NewServerInfoService(irods.NewServerInfoServiceWithFactory(*cfg, factory)),
		restservice.NewResourceService(irods.NewResourceServiceWithFactory(*cfg, factory)),
		restservice.NewUserService(irods.NewUserServiceWithFactory(*cfg, factory)),
		restservice.NewUserGroupService(irods.NewUserGroupServiceWithFactory(*cfg, factory)),
		restservice.NewUsersAndGroupsService(irods.NewUsersAndGroupsServiceWithFactory(*cfg, factory)),
		restservice.NewTicketService(irods.NewTicketServiceWithFactory(*cfg, factory)),
		stubAuthService{},
		stubAuthService{},
		auth.NewSessionStore(),
	), filesystem
}

type testCatalogFileSystem struct {
	entriesByPath      map[string]*irodsfs.Entry
	childrenByPath     map[string][]*irodsfs.Entry
	metadataByPath     map[string][]*irodstypes.IRODSMeta
	aclByPath          map[string][]*irodstypes.IRODSAccess
	inheritByPath      map[string]bool
	contentByPath      map[string][]byte
	ticketsByName      map[string]*irodstypes.IRODSTicket
	resources          []*irodstypes.IRODSResource
	serverVersion      *irodstypes.IRODSVersion
	usersByKey         map[string]*irodstypes.IRODSUser
	groupMembers       map[string][]string
	metadataByUser     map[string][]*irodstypes.IRODSMeta
	lastTrimMinCopies  int
	lastTrimMinAgeMins int
}

func newTestCatalogFileSystem() *testCatalogFileSystem {
	now := time.Unix(1_700_000_000, 0)

	zoneRoot := &irodsfs.Entry{
		ID:         90,
		Type:       irodsfs.DirectoryEntry,
		Name:       "tempZone",
		Path:       "/tempZone",
		CreateTime: now,
		ModifyTime: now,
	}
	homeRoot := &irodsfs.Entry{
		ID:         91,
		Type:       irodsfs.DirectoryEntry,
		Name:       "home",
		Path:       "/tempZone/home",
		CreateTime: now,
		ModifyTime: now,
	}
	userHome := &irodsfs.Entry{
		ID:         92,
		Type:       irodsfs.DirectoryEntry,
		Name:       "test1",
		Path:       "/tempZone/home/test1",
		CreateTime: now,
		ModifyTime: now,
	}
	project := &irodsfs.Entry{
		ID:         100,
		Type:       irodsfs.DirectoryEntry,
		Name:       "project",
		Path:       "/tempZone/home/test1/project",
		CreateTime: now,
		ModifyTime: now,
	}
	file := &irodsfs.Entry{
		ID:                101,
		Type:              irodsfs.FileEntry,
		Name:              "file.txt",
		Owner:             "rods",
		Path:              "/tempZone/home/test1/file.txt",
		Size:              128,
		DataType:          "generic",
		CheckSumAlgorithm: irodstypes.ChecksumAlgorithmSHA256,
		CheckSum:          []byte("abc123"),
		IRODSReplicas: []irodstypes.IRODSReplica{{
			Number:            0,
			Owner:             "rods",
			Status:            "1",
			ResourceName:      "demoResc",
			ResourceHierarchy: "demoResc",
			Path:              "/var/lib/irods/Vault/home/test1/file.txt",
			Checksum: &irodstypes.IRODSChecksum{
				Algorithm:           irodstypes.ChecksumAlgorithmSHA256,
				IRODSChecksumString: "sha2:YWJjMTIz",
			},
			ModifyTime: now,
		}},
		CreateTime: now,
		ModifyTime: now,
	}
	child := &irodsfs.Entry{
		ID:                102,
		Type:              irodsfs.FileEntry,
		Name:              "child.txt",
		Owner:             "alice",
		Path:              "/tempZone/home/test1/project/child.txt",
		Size:              64,
		DataType:          "generic",
		CheckSumAlgorithm: irodstypes.ChecksumAlgorithmSHA256,
		CheckSum:          []byte("childsum"),
		IRODSReplicas: []irodstypes.IRODSReplica{{
			Number:            2,
			Owner:             "alice",
			Status:            "2",
			ResourceName:      "repl1",
			ResourceHierarchy: "repl1;child1",
			Path:              "/var/lib/irods/child1vault/public/foo",
			ModifyTime:        now,
		}},
		CreateTime: now,
		ModifyTime: now,
	}
	nested := &irodsfs.Entry{
		ID:         103,
		Type:       irodsfs.DirectoryEntry,
		Name:       "nested",
		Path:       "/tempZone/home/test1/project/nested",
		CreateTime: now,
		ModifyTime: now,
	}

	return &testCatalogFileSystem{
		entriesByPath: map[string]*irodsfs.Entry{
			zoneRoot.Path: zoneRoot,
			homeRoot.Path: homeRoot,
			userHome.Path: userHome,
			project.Path:  project,
			file.Path:     file,
			child.Path:    child,
			nested.Path:   nested,
		},
		childrenByPath: map[string][]*irodsfs.Entry{
			zoneRoot.Path: {homeRoot},
			homeRoot.Path: {userHome},
			userHome.Path: {project, file},
			project.Path:  {child, nested},
		},
		metadataByPath: map[string][]*irodstypes.IRODSMeta{
			project.Path: {{
				AVUID:      700,
				Name:       "source",
				Value:      "test",
				Units:      "folder",
				CreateTime: now,
				ModifyTime: now,
			}},
			file.Path: {{
				AVUID:      701,
				Name:       "source",
				Value:      "test",
				Units:      "fixture",
				CreateTime: now,
				ModifyTime: now,
			}},
		},
		aclByPath: map[string][]*irodstypes.IRODSAccess{
			file.Path: {
				{
					Path:        file.Path,
					UserName:    "alice",
					UserZone:    "tempZone",
					UserType:    irodstypes.IRODSUserRodsUser,
					AccessLevel: irodstypes.IRODSAccessLevelOwner,
				},
				{
					Path:        file.Path,
					UserName:    "research-team",
					UserZone:    "tempZone",
					UserType:    irodstypes.IRODSUserRodsGroup,
					AccessLevel: irodstypes.IRODSAccessLevelReadObject,
				},
			},
			project.Path: {
				{
					Path:        project.Path,
					UserName:    "alice",
					UserZone:    "tempZone",
					UserType:    irodstypes.IRODSUserRodsUser,
					AccessLevel: irodstypes.IRODSAccessLevelOwner,
				},
			},
		},
		inheritByPath: map[string]bool{
			project.Path: false,
			nested.Path:  true,
		},
		contentByPath: map[string][]byte{
			file.Path:  []byte("hello content payload"),
			child.Path: []byte("child content payload"),
		},
		ticketsByName: map[string]*irodstypes.IRODSTicket{
			"ticket-existing": {
				ID:             900,
				Name:           "ticket-existing",
				Type:           irodstypes.TicketTypeRead,
				Owner:          "alice",
				OwnerZone:      "tempZone",
				ObjectType:     "data",
				Path:           file.Path,
				UsesLimit:      5,
				UsesCount:      1,
				WriteFileLimit: 10,
				ExpirationTime: now.Add(30 * time.Minute),
			},
		},
		resources: []*irodstypes.IRODSResource{
			{
				RescID:     500,
				Name:       "demoResc",
				Zone:       "tempZone",
				Type:       "unixfilesystem",
				Class:      "cache",
				Location:   "irods.example.org",
				Path:       "/var/lib/irods/Vault",
				Context:    "",
				CreateTime: now,
				ModifyTime: now,
			},
		},
		serverVersion: &irodstypes.IRODSVersion{
			ReleaseVersion: "rods4.3.2",
			APIVersion:     "d",
			ReconnectPort:  1247,
			ReconnectAddr:  "irods.example.org",
			Cookie:         734,
		},
		usersByKey: map[string]*irodstypes.IRODSUser{
			userKey("alice", "tempZone"): {
				ID:   300,
				Name: "alice",
				Zone: "tempZone",
				Type: irodstypes.IRODSUserRodsUser,
			},
			userKey("alicia", "tempZone"): {
				ID:   301,
				Name: "alicia",
				Zone: "tempZone",
				Type: irodstypes.IRODSUserRodsUser,
			},
			userKey("bob", "tempZone"): {
				ID:   302,
				Name: "bob",
				Zone: "tempZone",
				Type: irodstypes.IRODSUserRodsUser,
			},
			userKey("rods", "tempZone"): {
				ID:   303,
				Name: "rods",
				Zone: "tempZone",
				Type: irodstypes.IRODSUserRodsAdmin,
			},
			userKey("otheradmin", "tempZone"): {
				ID:   306,
				Name: "otheradmin",
				Zone: "tempZone",
				Type: irodstypes.IRODSUserRodsAdmin,
			},
			userKey("groupadmin", "tempZone"): {
				ID:   304,
				Name: "groupadmin",
				Zone: "tempZone",
				Type: irodstypes.IRODSUserGroupAdmin,
			},
			userKey("research-team", "tempZone"): {
				ID:   305,
				Name: "research-team",
				Zone: "tempZone",
				Type: irodstypes.IRODSUserRodsGroup,
			},
		},
		groupMembers: map[string][]string{
			userKey("research-team", "tempZone"): {"alice"},
		},
		metadataByUser: map[string][]*irodstypes.IRODSMeta{},
	}
}

func (f *testCatalogFileSystem) Stat(irodsPath string) (*irodsfs.Entry, error) {
	if irodsPath == "/tempZone/home/test1/forbidden" {
		return nil, irodstypes.NewIRODSError(irodscommon.CAT_NO_ACCESS_PERMISSION)
	}
	entry, ok := f.entriesByPath[irodsPath]
	if !ok {
		return nil, errors.New("not found")
	}
	return entry, nil
}

func (f *testCatalogFileSystem) List(irodsPath string) ([]*irodsfs.Entry, error) {
	entries, ok := f.childrenByPath[irodsPath]
	if !ok {
		return []*irodsfs.Entry{}, nil
	}
	return entries, nil
}

func (f *testCatalogFileSystem) MakeDir(irodsPath string, recurse bool) error {
	cleanPath := path.Clean(irodsPath)
	parentPath := path.Dir(cleanPath)
	if recurse && parentPath != "." && parentPath != "/" {
		if _, ok := f.entriesByPath[parentPath]; !ok {
			if err := f.MakeDir(parentPath, true); err != nil {
				return err
			}
		}
	}
	if parentPath != "." && parentPath != "/" {
		parentEntry, ok := f.entriesByPath[parentPath]
		if !ok || !parentEntry.IsDir() {
			return errors.New("not found")
		}
	}

	now := time.Unix(1_700_000_002, 0)
	entry := &irodsfs.Entry{
		ID:         int64(len(f.entriesByPath) + 200),
		Type:       irodsfs.DirectoryEntry,
		Name:       path.Base(cleanPath),
		Path:       cleanPath,
		CreateTime: now,
		ModifyTime: now,
	}
	f.entriesByPath[entry.Path] = entry
	f.childrenByPath[entry.Path] = []*irodsfs.Entry{}
	if parentPath != "." && parentPath != "/" {
		f.childrenByPath[parentPath] = append(f.childrenByPath[parentPath], entry)
	}
	return nil
}

func (f *testCatalogFileSystem) CreateFile(irodsPath string, _ string, _ string) (irods.CatalogFileHandle, error) {
	cleanPath := path.Clean(irodsPath)
	parentPath := path.Dir(path.Clean(irodsPath))
	if _, ok := f.entriesByPath[parentPath]; !ok {
		return nil, errors.New("not found")
	}

	if existing, ok := f.entriesByPath[cleanPath]; ok {
		if existing.IsDir() {
			return nil, errors.New("already exists")
		}
		f.contentByPath[cleanPath] = nil
		return &testCatalogFileHandle{
			reader: bytes.NewReader(nil),
			writer: bytes.NewBuffer(nil),
			onClose: func(data []byte) {
				f.contentByPath[cleanPath] = append([]byte(nil), data...)
				existing.Size = int64(len(data))
				existing.ModifyTime = time.Unix(1_700_000_002, 0)
			},
		}, nil
	}

	now := time.Unix(1_700_000_002, 0)
	entry := &irodsfs.Entry{
		ID:         int64(len(f.entriesByPath) + 200),
		Type:       irodsfs.FileEntry,
		Name:       path.Base(irodsPath),
		Path:       cleanPath,
		Owner:      "alice",
		Size:       0,
		DataType:   "generic",
		CreateTime: now,
		ModifyTime: now,
	}
	f.entriesByPath[entry.Path] = entry
	f.childrenByPath[parentPath] = append(f.childrenByPath[parentPath], entry)
	f.contentByPath[entry.Path] = nil

	return &testCatalogFileHandle{
		reader: bytes.NewReader(nil),
		writer: bytes.NewBuffer(nil),
		onClose: func(data []byte) {
			f.contentByPath[entry.Path] = append([]byte(nil), data...)
			entry.Size = int64(len(data))
		},
	}, nil
}

func (f *testCatalogFileSystem) RemoveDir(irodsPath string, recurse bool, _ bool) error {
	entry, ok := f.entriesByPath[irodsPath]
	if !ok || !entry.IsDir() {
		return errors.New("not found")
	}
	if !recurse {
		if children := f.childrenByPath[irodsPath]; len(children) > 0 {
			return errors.New("collection not empty")
		}
	}
	f.removeDirRecursive(path.Clean(irodsPath))
	return nil
}

func (f *testCatalogFileSystem) RemoveFile(irodsPath string, _ bool) error {
	entry, ok := f.entriesByPath[irodsPath]
	if !ok || entry.IsDir() {
		return errors.New("not found")
	}

	delete(f.entriesByPath, path.Clean(irodsPath))
	delete(f.contentByPath, path.Clean(irodsPath))
	delete(f.metadataByPath, path.Clean(irodsPath))
	parentPath := path.Dir(path.Clean(irodsPath))
	f.childrenByPath[parentPath] = filterChildEntry(f.childrenByPath[parentPath], path.Clean(irodsPath))
	return nil
}

func (f *testCatalogFileSystem) RenameDir(srcPath string, destPath string) error {
	entry, ok := f.entriesByPath[srcPath]
	if !ok || !entry.IsDir() {
		return errors.New("not found")
	}
	f.renameDirRecursive(path.Clean(srcPath), path.Clean(destPath))
	return nil
}

func (f *testCatalogFileSystem) RenameFile(srcPath string, destPath string) error {
	entry, ok := f.entriesByPath[srcPath]
	if !ok || entry.IsDir() {
		return errors.New("not found")
	}

	cleanSrc := path.Clean(srcPath)
	cleanDest := path.Clean(destPath)
	parentSrc := path.Dir(cleanSrc)
	parentDest := path.Dir(cleanDest)

	entry.Path = cleanDest
	entry.Name = path.Base(cleanDest)
	f.entriesByPath[cleanDest] = entry
	delete(f.entriesByPath, cleanSrc)

	if data, ok := f.contentByPath[cleanSrc]; ok {
		f.contentByPath[cleanDest] = data
		delete(f.contentByPath, cleanSrc)
	}
	if metas, ok := f.metadataByPath[cleanSrc]; ok {
		f.metadataByPath[cleanDest] = metas
		delete(f.metadataByPath, cleanSrc)
	}

	f.childrenByPath[parentSrc] = filterChildEntry(f.childrenByPath[parentSrc], cleanSrc)
	f.childrenByPath[parentDest] = append(f.childrenByPath[parentDest], entry)
	return nil
}

func (f *testCatalogFileSystem) CopyFile(srcPath string, destPath string, force bool) error {
	entry, ok := f.entriesByPath[srcPath]
	if !ok || entry.IsDir() {
		return errors.New("not found")
	}

	cleanDest := path.Clean(destPath)
	if _, exists := f.entriesByPath[cleanDest]; exists && !force {
		return errors.New("already exists")
	}

	parentDest := path.Dir(cleanDest)
	parentEntry, ok := f.entriesByPath[parentDest]
	if !ok || !parentEntry.IsDir() {
		return errors.New("not found")
	}

	cloned := *entry
	cloned.Path = cleanDest
	cloned.Name = path.Base(cleanDest)
	f.entriesByPath[cleanDest] = &cloned
	f.childrenByPath[parentDest] = append(f.childrenByPath[parentDest], &cloned)

	if data, ok := f.contentByPath[path.Clean(srcPath)]; ok {
		f.contentByPath[cleanDest] = append([]byte(nil), data...)
	}
	if metas, ok := f.metadataByPath[path.Clean(srcPath)]; ok {
		clonedMetas := make([]*irodstypes.IRODSMeta, 0, len(metas))
		for _, meta := range metas {
			if meta == nil {
				continue
			}
			cloneMeta := *meta
			clonedMetas = append(clonedMetas, &cloneMeta)
		}
		f.metadataByPath[cleanDest] = clonedMetas
	}

	return nil
}

func (f *testCatalogFileSystem) ReplicateFile(irodsPath string, resource string, _ bool) error {
	resource = strings.TrimSpace(resource)
	if resource == "" {
		return errors.New("resource is required")
	}

	entry, ok := f.entriesByPath[irodsPath]
	if !ok || entry.IsDir() {
		return errors.New("not found")
	}

	for _, replica := range entry.IRODSReplicas {
		if strings.TrimSpace(replica.ResourceName) == resource {
			return nil
		}
	}

	nextReplicaNumber := int64(0)
	for _, replica := range entry.IRODSReplicas {
		if replica.Number >= nextReplicaNumber {
			nextReplicaNumber = replica.Number + 1
		}
	}

	now := time.Unix(1_700_000_003, 0)
	entry.IRODSReplicas = append(entry.IRODSReplicas, irodstypes.IRODSReplica{
		Number:            nextReplicaNumber,
		Owner:             entry.Owner,
		Status:            "1",
		ResourceName:      resource,
		ResourceHierarchy: resource,
		Path:              "/var/lib/irods/" + resource + "/Vault" + entry.Path,
		ModifyTime:        now,
	})
	entry.ModifyTime = now
	return nil
}

func (f *testCatalogFileSystem) TrimDataObject(irodsPath string, resource string, minCopies int, minAgeMinutes int) error {
	resource = strings.TrimSpace(resource)
	if resource == "" {
		return errors.New("resource is required")
	}
	f.lastTrimMinCopies = minCopies
	f.lastTrimMinAgeMins = minAgeMinutes

	entry, ok := f.entriesByPath[irodsPath]
	if !ok || entry.IsDir() {
		return errors.New("not found")
	}

	if minCopies < 0 {
		minCopies = 0
	}

	replicas := entry.IRODSReplicas
	filtered := make([]irodstypes.IRODSReplica, 0, len(replicas))
	removed := false

	for _, replica := range replicas {
		if !removed && strings.TrimSpace(replica.ResourceName) == resource && len(replicas)-1 >= minCopies {
			removed = true
			continue
		}
		filtered = append(filtered, replica)
	}

	if !removed {
		return errors.New("not found")
	}

	entry.IRODSReplicas = filtered
	entry.ModifyTime = time.Unix(1_700_000_004, 0)
	return nil
}

func (f *testCatalogFileSystem) ListMetadata(irodsPath string) ([]*irodstypes.IRODSMeta, error) {
	return f.metadataByPath[irodsPath], nil
}

func (f *testCatalogFileSystem) SearchByMeta(metaName string, metaValue string) ([]s3adminext.Entry, error) {
	entries := make([]s3adminext.Entry, 0)
	for irodsPath, metadataList := range f.metadataByPath {
		entry := f.entriesByPath[irodsPath]
		if entry == nil {
			continue
		}

		for _, metadata := range metadataList {
			if metadata == nil || metadata.Name != metaName {
				continue
			}
			if metaValue != "%" && metadata.Value != metaValue {
				continue
			}

			entryType := s3adminext.EntryTypeFile
			if entry.IsDir() {
				entryType = s3adminext.EntryTypeDirectory
			}
			entries = append(entries, s3adminext.Entry{
				Path: entry.Path,
				Type: entryType,
			})
			break
		}
	}
	return entries, nil
}

func (f *testCatalogFileSystem) QueryMetadataEntries(query metadataext.EntryQuery) (metadataext.EntryQueryResult, error) {
	return queryMetadataEntriesForTest(f.entriesByPath, f.metadataByPath, query)
}

func queryMetadataEntriesForTest(entriesByPath map[string]*irodsfs.Entry, metadataByPath map[string][]*irodstypes.IRODSMeta, query metadataext.EntryQuery) (metadataext.EntryQueryResult, error) {
	normalized, err := metadataext.NormalizeEntryQuery(query)
	if err != nil {
		return metadataext.EntryQueryResult{}, err
	}

	collections := matchingMetadataQueryEntriesForTest(entriesByPath, metadataByPath, normalized, metadataext.EntryKindCollection)
	dataObjects := matchingMetadataQueryEntriesForTest(entriesByPath, metadataByPath, normalized, metadataext.EntryKindDataObject)

	result := metadataext.EntryQueryResult{
		Entries: []*metadataext.Entry{},
		Page: metadataext.EntryQueryPage{
			Limit: normalized.Limit,
		},
	}
	if normalized.IncludeMatchedAVUs {
		result.MatchedAVUs = map[string][]metadataext.AVUStat{}
	}

	cursor := metadataext.EntryQueryCursor{}
	if normalized.Cursor != nil {
		cursor = *normalized.Cursor
	}
	phase := cursor.Phase
	if phase == "" {
		if metadataext.EntryQueryHasKind(normalized, metadataext.EntryKindCollection) {
			phase = metadataext.EntryQueryPhaseCollections
		} else {
			phase = metadataext.EntryQueryPhaseDataObjects
		}
	}

	next := cursor
	remaining := normalized.Limit
	hasMore := false
	if phase == metadataext.EntryQueryPhaseCollections && metadataext.EntryQueryHasKind(normalized, metadataext.EntryKindCollection) && !cursor.Collections.Exhausted {
		added := appendMetadataQueryPageForTest(&result, collections, cursor.Collections.Offset, remaining, normalized.IncludeMatchedAVUs)
		next.Collections.Offset += added
		result.Page.Returned.Collections += added
		result.Page.Scanned.Collections = len(collections)
		remaining -= added
		if next.Collections.Offset < len(collections) {
			hasMore = true
			next.Phase = metadataext.EntryQueryPhaseCollections
		} else {
			next.Collections.Exhausted = true
			phase = metadataext.EntryQueryPhaseDataObjects
			next.Phase = metadataext.EntryQueryPhaseDataObjects
		}
	}
	if !hasMore && phase == metadataext.EntryQueryPhaseDataObjects && metadataext.EntryQueryHasKind(normalized, metadataext.EntryKindDataObject) && !cursor.DataObjects.Exhausted {
		added := appendMetadataQueryPageForTest(&result, dataObjects, cursor.DataObjects.Offset, remaining, normalized.IncludeMatchedAVUs)
		next.DataObjects.Offset += added
		result.Page.Returned.DataObjects += added
		result.Page.Scanned.DataObjects = len(dataObjects)
		remaining -= added
		if next.DataObjects.Offset < len(dataObjects) {
			hasMore = true
			next.Phase = metadataext.EntryQueryPhaseDataObjects
		} else {
			next.DataObjects.Exhausted = true
			next.Phase = metadataext.EntryQueryPhaseDone
		}
	}
	if !hasMore && remaining == 0 && phase == metadataext.EntryQueryPhaseDataObjects && metadataext.EntryQueryHasKind(normalized, metadataext.EntryKindDataObject) && !next.DataObjects.Exhausted && next.DataObjects.Offset < len(dataObjects) {
		hasMore = true
		next.Phase = metadataext.EntryQueryPhaseDataObjects
	}

	if normalized.IncludeTotals {
		totals := metadataext.EntryQueryCounts{
			Collections: len(collections),
			DataObjects: len(dataObjects),
		}
		result.Page.Totals = &totals
	}
	result.Page.HasMore = hasMore
	if hasMore {
		result.Page.Next = &next
	}
	return result, nil
}

type metadataQueryTestMatch struct {
	entry *metadataext.Entry
	avus  []metadataext.AVUStat
}

func matchingMetadataQueryEntriesForTest(entriesByPath map[string]*irodsfs.Entry, metadataByPath map[string][]*irodstypes.IRODSMeta, query metadataext.EntryQuery, kind metadataext.EntryKind) []metadataQueryTestMatch {
	if !metadataext.EntryQueryHasKind(query, kind) {
		return nil
	}

	paths := make([]string, 0, len(entriesByPath))
	for irodsPath := range entriesByPath {
		paths = append(paths, irodsPath)
	}
	sort.Strings(paths)

	matches := []metadataQueryTestMatch{}
	for _, irodsPath := range paths {
		entry := entriesByPath[irodsPath]
		if entry == nil {
			continue
		}
		if kind == metadataext.EntryKindCollection && !entry.IsDir() {
			continue
		}
		if kind == metadataext.EntryKindDataObject && entry.IsDir() {
			continue
		}
		if !metadataQueryScopeMatchesForTest(entry, query.Scope) {
			continue
		}
		avus, ok := metadataQueryConditionsMatchForTest(entry, metadataByPath[entry.Path], query.Conditions)
		if !ok {
			continue
		}
		matches = append(matches, metadataQueryTestMatch{entry: entry, avus: avus})
	}
	return matches
}

func appendMetadataQueryPageForTest(result *metadataext.EntryQueryResult, matches []metadataQueryTestMatch, offset int, limit int, includeMatchedAVUs bool) int {
	if limit <= 0 || offset >= len(matches) {
		return 0
	}
	added := 0
	for idx := offset; idx < len(matches) && added < limit; idx++ {
		match := matches[idx]
		result.Entries = append(result.Entries, match.entry)
		added++
		if includeMatchedAVUs {
			for _, avu := range match.avus {
				result.MatchedAVUs[match.entry.Path] = append(result.MatchedAVUs[match.entry.Path], avu)
			}
		}
	}
	return added
}

func metadataQueryScopeMatchesForTest(entry *irodsfs.Entry, scope *metadataext.EntryQueryScope) bool {
	if scope == nil || scope.Mode == metadataext.EntryQueryScopeAbsolute {
		return true
	}
	root := strings.TrimRight(scope.Root, "/")
	switch scope.Mode {
	case metadataext.EntryQueryScopeSelf:
		return entry.IsDir() && entry.Path == root
	case metadataext.EntryQueryScopeChildren:
		return path.Dir(path.Clean(entry.Path)) == root
	case metadataext.EntryQueryScopeDescendants:
		if entry.IsDir() {
			return strings.HasPrefix(path.Clean(entry.Path), root+"/")
		}
		return strings.HasPrefix(path.Dir(path.Clean(entry.Path)), root+"/")
	default:
		return false
	}
}

func metadataQueryConditionsMatchForTest(entry *irodsfs.Entry, metadataList []*irodstypes.IRODSMeta, conditions []metadataext.EntryCondition) ([]metadataext.AVUStat, bool) {
	avuConditions := []metadataext.EntryCondition{}
	for _, condition := range conditions {
		switch condition.Field {
		case metadataext.FieldAVUAttrib, metadataext.FieldAVUValue, metadataext.FieldAVUUnit:
			avuConditions = append(avuConditions, condition)
		default:
			if !metadataQueryEntryConditionMatchesForTest(entry, condition) {
				return nil, false
			}
		}
	}

	if len(avuConditions) == 0 {
		return nil, true
	}

	matchedAVUs := []metadataext.AVUStat{}
	for _, avu := range metadataList {
		if avu == nil {
			continue
		}
		if metadataQueryAVUMatchesForTest(avu, avuConditions) {
			matchedAVUs = append(matchedAVUs, metadataext.AVUStat{
				ID:         avu.AVUID,
				Name:       avu.Name,
				Value:      avu.Value,
				Units:      avu.Units,
				CreateTime: avu.CreateTime,
				ModifyTime: avu.ModifyTime,
			})
		}
	}
	return matchedAVUs, len(matchedAVUs) > 0
}

func metadataQueryEntryConditionMatchesForTest(entry *irodsfs.Entry, condition metadataext.EntryCondition) bool {
	switch condition.Field {
	case metadataext.FieldPath:
		return metadataQueryStringMatchesForTest(entry.Path, condition)
	case metadataext.FieldName:
		return metadataQueryStringMatchesForTest(entry.Name, condition)
	case metadataext.FieldOwner:
		return metadataQueryStringMatchesForTest(entry.Owner, condition)
	case metadataext.FieldDataType:
		return metadataQueryStringMatchesForTest(entry.DataType, condition)
	case metadataext.FieldResource:
		return metadataQueryStringMatchesForTest(firstReplicaResourceNameForTest(entry), condition)
	case metadataext.FieldChecksum:
		return metadataQueryStringMatchesForTest(string(entry.CheckSum), condition)
	default:
		return false
	}
}

func metadataQueryAVUMatchesForTest(avu *irodstypes.IRODSMeta, conditions []metadataext.EntryCondition) bool {
	for _, condition := range conditions {
		var value string
		switch condition.Field {
		case metadataext.FieldAVUAttrib:
			value = avu.Name
		case metadataext.FieldAVUValue:
			value = avu.Value
		case metadataext.FieldAVUUnit:
			value = avu.Units
		default:
			return false
		}
		if !metadataQueryStringMatchesForTest(value, condition) {
			return false
		}
	}
	return true
}

func metadataQueryStringMatchesForTest(value string, condition metadataext.EntryCondition) bool {
	switch condition.Op {
	case metadataext.OpEqual:
		return value == condition.Value
	case metadataext.OpLike:
		pattern := strings.ReplaceAll(condition.Value, "%", "*")
		ok, err := path.Match(pattern, value)
		return err == nil && ok
	default:
		return false
	}
}

func firstReplicaResourceNameForTest(entry *irodsfs.Entry) string {
	if entry == nil || len(entry.IRODSReplicas) == 0 {
		return ""
	}
	return entry.IRODSReplicas[0].ResourceName
}

func (f *testCatalogFileSystem) AddMetadata(irodsPath string, attName string, attValue string, attUnits string) error {
	if _, ok := f.entriesByPath[irodsPath]; !ok {
		return errors.New("not found")
	}

	nextID := int64(1)
	for _, meta := range f.metadataByPath[irodsPath] {
		if meta != nil && meta.AVUID >= nextID {
			nextID = meta.AVUID + 1
		}
	}

	now := time.Unix(1_700_000_001, 0)
	f.metadataByPath[irodsPath] = append(f.metadataByPath[irodsPath], &irodstypes.IRODSMeta{
		AVUID:      nextID,
		Name:       attName,
		Value:      attValue,
		Units:      attUnits,
		CreateTime: now,
		ModifyTime: now,
	})
	return nil
}

func (f *testCatalogFileSystem) ReplaceMetadataByID(irodsPath string, avuID int64, target metadataext.AVUStat) (metadataext.AVUStat, error) {
	if _, ok := f.entriesByPath[irodsPath]; !ok {
		return metadataext.AVUStat{}, errors.New("not found")
	}

	for _, meta := range f.metadataByPath[irodsPath] {
		if meta == nil {
			continue
		}
		if meta.AVUID == avuID {
			meta.Name = target.Name
			meta.Value = target.Value
			meta.Units = target.Units
			meta.ModifyTime = time.Unix(1_700_000_002, 0)
			return metadataext.AVUStat{ID: meta.AVUID, Name: meta.Name, Value: meta.Value, Units: meta.Units, CreateTime: meta.CreateTime, ModifyTime: meta.ModifyTime}, nil
		}
	}

	return metadataext.AVUStat{}, errors.New("not found")
}

func (f *testCatalogFileSystem) DeleteMetadata(irodsPath string, avuID int64) error {
	metas := f.metadataByPath[irodsPath]
	filtered := metas[:0]
	found := false
	for _, meta := range metas {
		if meta != nil && meta.AVUID == avuID {
			found = true
			continue
		}
		filtered = append(filtered, meta)
	}
	if !found {
		return errors.New("not found")
	}
	f.metadataByPath[irodsPath] = filtered
	return nil
}

func (f *testCatalogFileSystem) ListACLs(irodsPath string) ([]*irodstypes.IRODSAccess, error) {
	if _, ok := f.entriesByPath[irodsPath]; !ok {
		return nil, errors.New("not found")
	}
	return f.aclByPath[irodsPath], nil
}

func (f *testCatalogFileSystem) ChangeACLs(irodsPath string, access irodstypes.IRODSAccessLevelType, userName string, zoneName string, _ bool, _ bool) error {
	if _, ok := f.entriesByPath[irodsPath]; !ok {
		return errors.New("not found")
	}

	current := f.aclByPath[irodsPath]
	filtered := current[:0]
	for _, acl := range current {
		if acl != nil && acl.UserName == userName && acl.UserZone == zoneName {
			continue
		}
		filtered = append(filtered, acl)
	}
	f.aclByPath[irodsPath] = filtered

	if access == irodstypes.IRODSAccessLevelNull {
		return nil
	}

	user, ok := f.usersByKey[userKey(userName, zoneName)]
	userType := irodstypes.IRODSUserRodsUser
	if ok && user != nil {
		userType = user.Type
	}

	f.aclByPath[irodsPath] = append(f.aclByPath[irodsPath], &irodstypes.IRODSAccess{
		Path:        irodsPath,
		UserName:    userName,
		UserZone:    zoneName,
		UserType:    userType,
		AccessLevel: access,
	})
	return nil
}

func (f *testCatalogFileSystem) ChangeDirACLInheritance(irodsPath string, inherit bool, _ bool, _ bool) error {
	entry, ok := f.entriesByPath[irodsPath]
	if !ok || !entry.IsDir() {
		return errors.New("not found")
	}
	f.inheritByPath[irodsPath] = inherit
	return nil
}

func (f *testCatalogFileSystem) GetDirACLInheritance(irodsPath string) (*irodstypes.IRODSAccessInheritance, error) {
	entry, ok := f.entriesByPath[irodsPath]
	if !ok || !entry.IsDir() {
		return nil, errors.New("not found")
	}
	value := f.inheritByPath[irodsPath]
	return &irodstypes.IRODSAccessInheritance{
		Path:        irodsPath,
		Inheritance: value,
	}, nil
}

func (f *testCatalogFileSystem) ComputeChecksum(irodsPath string, _ string) (*irodstypes.IRODSChecksum, error) {
	entry, ok := f.entriesByPath[irodsPath]
	if !ok {
		return nil, errors.New("not found")
	}
	if entry.IsDir() {
		return nil, errors.New("not found")
	}

	var checksum string
	switch irodsPath {
	case "/tempZone/home/test1/project/child.txt":
		checksum = "sha2:Y2hpbGQtY29tcHV0ZWQ="
		entry.CheckSum = []byte("child-computed")
	default:
		checksum = "sha2:YWJjMTIz"
		entry.CheckSum = []byte("abc123")
	}

	entry.CheckSumAlgorithm = irodstypes.ChecksumAlgorithmSHA256
	if len(entry.IRODSReplicas) > 0 {
		entry.IRODSReplicas[0].Checksum = &irodstypes.IRODSChecksum{
			Algorithm:           irodstypes.ChecksumAlgorithmSHA256,
			IRODSChecksumString: checksum,
		}
	}

	return &irodstypes.IRODSChecksum{
		Algorithm:           irodstypes.ChecksumAlgorithmSHA256,
		IRODSChecksumString: checksum,
	}, nil
}

func (f *testCatalogFileSystem) GetServerVersion() (*irodstypes.IRODSVersion, error) {
	if f.serverVersion == nil {
		return &irodstypes.IRODSVersion{}, nil
	}

	copy := *f.serverVersion
	return &copy, nil
}

func (f *testCatalogFileSystem) OpenFile(irodsPath string, _ string, _ string) (irods.CatalogFileHandle, error) {
	data, ok := f.contentByPath[irodsPath]
	if !ok {
		return nil, errors.New("not found")
	}

	return &testCatalogFileHandle{reader: bytes.NewReader(data)}, nil
}

func (f *testCatalogFileSystem) ListResources() ([]*irodstypes.IRODSResource, error) {
	return f.resources, nil
}

func (f *testCatalogFileSystem) GetResource(resourceName string) (*irodstypes.IRODSResource, error) {
	for _, resource := range f.resources {
		if resource != nil && resource.Name == resourceName {
			return resource, nil
		}
	}
	return nil, irodstypes.NewResourceNotFoundError(resourceName)
}

func (f *testCatalogFileSystem) GetUser(username string, zoneName string, _ irodstypes.IRODSUserType) (*irodstypes.IRODSUser, error) {
	user, ok := f.usersByKey[userKey(username, zoneName)]
	if !ok {
		return nil, irodstypes.NewUserNotFoundError(username)
	}
	return user, nil
}

func (f *testCatalogFileSystem) ListUsers(zoneName string, userType irodstypes.IRODSUserType) ([]*irodstypes.IRODSUser, error) {
	users := make([]*irodstypes.IRODSUser, 0, len(f.usersByKey))
	for _, user := range f.usersByKey {
		if user == nil || user.Zone != zoneName || user.Type != userType {
			continue
		}
		users = append(users, user)
	}
	return users, nil
}

func (f *testCatalogFileSystem) ListGroupMembers(zoneName string, groupName string) ([]*irodstypes.IRODSUser, error) {
	key := userKey(groupName, zoneName)
	usernames := f.groupMembers[key]
	members := make([]*irodstypes.IRODSUser, 0, len(usernames))
	for _, username := range usernames {
		user, ok := f.usersByKey[userKey(username, zoneName)]
		if !ok {
			continue
		}
		members = append(members, user)
	}
	return members, nil
}

func (f *testCatalogFileSystem) ListUserMetadata(username string, zoneName string) ([]*irodstypes.IRODSMeta, error) {
	if _, ok := f.usersByKey[userKey(username, zoneName)]; !ok {
		return nil, irodstypes.NewUserNotFoundError(username)
	}

	metadata := f.metadataByUser[userKey(username, zoneName)]
	result := make([]*irodstypes.IRODSMeta, 0, len(metadata))
	for _, meta := range metadata {
		if meta == nil {
			continue
		}
		copy := *meta
		result = append(result, &copy)
	}
	return result, nil
}

func (f *testCatalogFileSystem) AddUserMetadata(username string, zoneName string, attribute string, value string, unit string) error {
	if _, ok := f.usersByKey[userKey(username, zoneName)]; !ok {
		return irodstypes.NewUserNotFoundError(username)
	}
	if f.hasUserMetadata(username, zoneName, attribute, value, unit) {
		return errors.New("already exists")
	}

	f.addUserMetadata(username, zoneName, attribute, value, unit)
	return nil
}

func (f *testCatalogFileSystem) ReplaceUserMetadataByID(username string, zoneName string, avuID int64, target metadataext.AVUStat) (metadataext.AVUStat, error) {
	if _, ok := f.usersByKey[userKey(username, zoneName)]; !ok {
		return metadataext.AVUStat{}, irodstypes.NewUserNotFoundError(username)
	}

	metadata := f.metadataByUser[userKey(username, zoneName)]
	for i, meta := range metadata {
		if meta != nil && meta.AVUID == avuID {
			meta.Name = target.Name
			meta.Value = target.Value
			meta.Units = target.Units
			meta.ModifyTime = time.Now().UTC()
			f.metadataByUser[userKey(username, zoneName)][i] = meta
			return metadataext.AVUStat{ID: meta.AVUID, Name: meta.Name, Value: meta.Value, Units: meta.Units, CreateTime: meta.CreateTime, ModifyTime: meta.ModifyTime}, nil
		}
	}
	return metadataext.AVUStat{}, metadataext.ErrAVUNotFound
}

func (f *testCatalogFileSystem) DeleteUserMetadata(username string, zoneName string, avuID int64) error {
	if _, ok := f.usersByKey[userKey(username, zoneName)]; !ok {
		return irodstypes.NewUserNotFoundError(username)
	}

	metadata := f.metadataByUser[userKey(username, zoneName)]
	filtered := make([]*irodstypes.IRODSMeta, 0, len(metadata))
	deleted := false
	for _, meta := range metadata {
		if meta != nil && meta.AVUID == avuID {
			deleted = true
			continue
		}
		filtered = append(filtered, meta)
	}
	if !deleted {
		return metadataext.ErrAVUNotFound
	}
	f.metadataByUser[userKey(username, zoneName)] = filtered
	return nil
}

func (f *testCatalogFileSystem) CreateUser(username string, zoneName string, userType irodstypes.IRODSUserType) (*irodstypes.IRODSUser, error) {
	key := userKey(username, zoneName)
	if existing, ok := f.usersByKey[key]; ok {
		return existing, errors.New("already exists")
	}

	user := &irodstypes.IRODSUser{
		ID:   int64(len(f.usersByKey) + 500),
		Name: username,
		Zone: zoneName,
		Type: userType,
	}
	f.usersByKey[key] = user
	return user, nil
}

func (f *testCatalogFileSystem) CreateUserGroup(groupName string, zoneName string) (*irodstypes.IRODSUser, error) {
	return f.CreateUser(groupName, zoneName, irodstypes.IRODSUserRodsGroup)
}

func (f *testCatalogFileSystem) ChangeUserPassword(username string, zoneName string, _ string) error {
	if _, ok := f.usersByKey[userKey(username, zoneName)]; !ok {
		return irodstypes.NewUserNotFoundError(username)
	}
	return nil
}

func (f *testCatalogFileSystem) ChangeUserType(username string, zoneName string, newType irodstypes.IRODSUserType) error {
	user, ok := f.usersByKey[userKey(username, zoneName)]
	if !ok {
		return irodstypes.NewUserNotFoundError(username)
	}
	user.Type = newType
	return nil
}

func (f *testCatalogFileSystem) RemoveUser(username string, zoneName string, _ irodstypes.IRODSUserType) error {
	key := userKey(username, zoneName)
	if _, ok := f.usersByKey[key]; !ok {
		return irodstypes.NewUserNotFoundError(username)
	}
	delete(f.usersByKey, key)
	delete(f.groupMembers, key)
	delete(f.metadataByUser, key)
	for groupKey, members := range f.groupMembers {
		filtered := members[:0]
		for _, member := range members {
			if member == username {
				continue
			}
			filtered = append(filtered, member)
		}
		f.groupMembers[groupKey] = filtered
	}
	return nil
}

func (f *testCatalogFileSystem) RemoveUserGroup(groupName string, zoneName string) error {
	return f.RemoveUser(groupName, zoneName, irodstypes.IRODSUserRodsGroup)
}

func (f *testCatalogFileSystem) AddGroupMember(groupName string, username string, zoneName string) error {
	group, ok := f.usersByKey[userKey(groupName, zoneName)]
	if !ok || group.Type != irodstypes.IRODSUserRodsGroup {
		return irodstypes.NewUserNotFoundError(groupName)
	}
	if _, ok := f.usersByKey[userKey(username, zoneName)]; !ok {
		return irodstypes.NewUserNotFoundError(username)
	}

	key := userKey(groupName, zoneName)
	members := f.groupMembers[key]
	for _, member := range members {
		if member == username {
			return nil
		}
	}
	f.groupMembers[key] = append(members, username)
	return nil
}

func (f *testCatalogFileSystem) RemoveGroupMember(groupName string, username string, zoneName string) error {
	key := userKey(groupName, zoneName)
	members, ok := f.groupMembers[key]
	if !ok {
		return irodstypes.NewUserNotFoundError(groupName)
	}

	filtered := members[:0]
	removed := false
	for _, member := range members {
		if member == username {
			removed = true
			continue
		}
		filtered = append(filtered, member)
	}
	if !removed {
		return irodstypes.NewUserNotFoundError(username)
	}
	f.groupMembers[key] = filtered
	return nil
}

func (f *testCatalogFileSystem) addUserMetadata(username string, zoneName string, attribute string, value string, unit string) {
	key := userKey(username, zoneName)
	f.metadataByUser[key] = append(f.metadataByUser[key], &irodstypes.IRODSMeta{
		AVUID: int64(len(f.metadataByUser[key]) + 1),
		Name:  attribute,
		Value: value,
		Units: unit,
	})
}

func (f *testCatalogFileSystem) hasUserMetadata(username string, zoneName string, attribute string, value string, unit string) bool {
	for _, meta := range f.metadataByUser[userKey(username, zoneName)] {
		if meta == nil {
			continue
		}
		if meta.Name == attribute && meta.Value == value && meta.Units == unit {
			return true
		}
	}
	return false
}

func (f *testCatalogFileSystem) Release() {}

func (f *testCatalogFileSystem) UsersAndGroupsCatalog() usersandgroupsext.Catalog {
	return testUsersAndGroupsCatalog{filesystem: f}
}

type testUsersAndGroupsCatalog struct {
	filesystem *testCatalogFileSystem
}

func (c testUsersAndGroupsCatalog) ListGroupSummaries(_ context.Context, options usersandgroupsext.GroupSummaryOptions) ([]usersandgroupsext.GroupSummary, error) {
	groups := make([]usersandgroupsext.GroupSummary, 0)
	for _, user := range c.filesystem.usersByKey {
		if user == nil || user.Zone != options.Zone || user.Type != irodstypes.IRODSUserRodsGroup {
			continue
		}
		if strings.TrimSpace(options.Prefix) != "" && !strings.HasPrefix(user.Name, options.Prefix) {
			continue
		}
		groups = append(groups, usersandgroupsext.GroupSummary{
			ID:          user.ID,
			Name:        user.Name,
			Zone:        user.Zone,
			Type:        user.Type,
			MemberCount: len(c.filesystem.groupMembers[userKey(user.Name, user.Zone)]),
		})
	}
	sort.SliceStable(groups, func(i, j int) bool {
		return groups[i].Name < groups[j].Name
	})
	if options.Limit > 0 && len(groups) > options.Limit {
		groups = groups[:options.Limit]
	}
	return groups, nil
}

func (c testUsersAndGroupsCatalog) ListUserMembershipSummaries(_ context.Context, options usersandgroupsext.UserMembershipSummaryOptions) ([]usersandgroupsext.UserMembershipSummary, error) {
	users := make([]usersandgroupsext.UserMembershipSummary, 0)
	for _, user := range c.filesystem.usersByKey {
		if user == nil || user.Zone != options.Zone || user.Type == irodstypes.IRODSUserRodsGroup {
			continue
		}
		if options.Type != "" && user.Type != options.Type {
			continue
		}
		if strings.TrimSpace(options.Prefix) != "" && !strings.HasPrefix(user.Name, options.Prefix) {
			continue
		}
		users = append(users, usersandgroupsext.UserMembershipSummary{
			ID:     user.ID,
			Name:   user.Name,
			Zone:   user.Zone,
			Type:   user.Type,
			Groups: c.groupsForUser(user.Name, user.Zone),
		})
	}
	sort.SliceStable(users, func(i, j int) bool {
		return users[i].Name < users[j].Name
	})
	if options.Limit > 0 && len(users) > options.Limit {
		users = users[:options.Limit]
	}
	return users, nil
}

func (c testUsersAndGroupsCatalog) ListGroupsForUser(_ context.Context, options usersandgroupsext.GroupsForUserOptions) ([]usersandgroupsext.GroupRef, error) {
	groups := c.groupsForUser(options.UserName, options.Zone)
	if options.Limit > 0 && len(groups) > options.Limit {
		groups = groups[:options.Limit]
	}
	return groups, nil
}

func (c testUsersAndGroupsCatalog) SearchPrincipals(_ context.Context, options usersandgroupsext.PrincipalSearchOptions) ([]usersandgroupsext.PrincipalSearchResult, error) {
	results := make([]usersandgroupsext.PrincipalSearchResult, 0)
	includeUsers := len(options.Kinds) == 0
	includeGroups := len(options.Kinds) == 0
	for _, kind := range options.Kinds {
		includeUsers = includeUsers || kind == usersandgroupsext.PrincipalKindUser
		includeGroups = includeGroups || kind == usersandgroupsext.PrincipalKindGroup
	}

	for _, user := range c.filesystem.usersByKey {
		if user == nil || user.Zone != options.Zone || !strings.HasPrefix(user.Name, options.Query) {
			continue
		}
		if user.Type == irodstypes.IRODSUserRodsGroup {
			if !includeGroups {
				continue
			}
			results = append(results, usersandgroupsext.PrincipalSearchResult{
				ID:   user.ID,
				Name: user.Name,
				Zone: user.Zone,
				Type: user.Type,
				Kind: usersandgroupsext.PrincipalKindGroup,
			})
			continue
		}
		if includeUsers {
			results = append(results, usersandgroupsext.PrincipalSearchResult{
				ID:   user.ID,
				Name: user.Name,
				Zone: user.Zone,
				Type: user.Type,
				Kind: usersandgroupsext.PrincipalKindUser,
			})
		}
	}
	sort.SliceStable(results, func(i, j int) bool {
		return results[i].Name < results[j].Name
	})
	if options.Limit > 0 && len(results) > options.Limit {
		results = results[:options.Limit]
	}
	return results, nil
}

func (c testUsersAndGroupsCatalog) groupsForUser(username string, zone string) []usersandgroupsext.GroupRef {
	groups := make([]usersandgroupsext.GroupRef, 0)
	for key, members := range c.filesystem.groupMembers {
		group, ok := c.filesystem.usersByKey[key]
		if !ok || group == nil || group.Zone != zone {
			continue
		}
		for _, member := range members {
			if member != username {
				continue
			}
			groups = append(groups, usersandgroupsext.GroupRef{
				ID:   group.ID,
				Name: group.Name,
				Zone: group.Zone,
				Type: group.Type,
			})
		}
	}
	sort.SliceStable(groups, func(i, j int) bool {
		return groups[i].Name < groups[j].Name
	})
	return groups
}

func (f *testCatalogFileSystem) GetTicket(ticketName string) (*irodstypes.IRODSTicket, error) {
	ticket, ok := f.ticketsByName[ticketName]
	if !ok {
		return nil, irodstypes.NewTicketNotFoundError(ticketName)
	}
	return ticket, nil
}

func (f *testCatalogFileSystem) ListTickets() ([]*irodstypes.IRODSTicket, error) {
	results := make([]*irodstypes.IRODSTicket, 0, len(f.ticketsByName))
	for _, ticket := range f.ticketsByName {
		results = append(results, ticket)
	}
	return results, nil
}

func (f *testCatalogFileSystem) CreateTicket(ticketName string, ticketType irodstypes.TicketType, irodsPath string) error {
	if _, ok := f.entriesByPath[irodsPath]; !ok {
		return errors.New("not found")
	}

	now := time.Unix(1_700_000_000, 0)
	f.ticketsByName[ticketName] = &irodstypes.IRODSTicket{
		ID:         int64(len(f.ticketsByName) + 1000),
		Name:       ticketName,
		Type:       ticketType,
		Owner:      "alice",
		OwnerZone:  "tempZone",
		ObjectType: "data",
		Path:       irodsPath,
	}
	f.entriesByPath[path.Clean(irodsPath)].ModifyTime = now
	return nil
}

func (f *testCatalogFileSystem) DeleteTicket(ticketName string) error {
	if _, ok := f.ticketsByName[ticketName]; !ok {
		return irodstypes.NewTicketNotFoundError(ticketName)
	}
	delete(f.ticketsByName, ticketName)
	return nil
}

func (f *testCatalogFileSystem) ModifyTicketUseLimit(ticketName string, uses int64) error {
	ticket, ok := f.ticketsByName[ticketName]
	if !ok {
		return irodstypes.NewTicketNotFoundError(ticketName)
	}
	ticket.UsesLimit = uses
	return nil
}

func (f *testCatalogFileSystem) ClearTicketUseLimit(ticketName string) error {
	return f.ModifyTicketUseLimit(ticketName, 0)
}

func (f *testCatalogFileSystem) ModifyTicketExpirationTime(ticketName string, expirationTime time.Time) error {
	ticket, ok := f.ticketsByName[ticketName]
	if !ok {
		return irodstypes.NewTicketNotFoundError(ticketName)
	}
	ticket.ExpirationTime = expirationTime
	return nil
}

func (f *testCatalogFileSystem) ClearTicketExpirationTime(ticketName string) error {
	return f.ModifyTicketExpirationTime(ticketName, time.Time{})
}

func (f *testCatalogFileSystem) removeDirRecursive(irodsPath string) {
	for _, child := range f.childrenByPath[irodsPath] {
		if child == nil {
			continue
		}
		if child.IsDir() {
			f.removeDirRecursive(child.Path)
			continue
		}
		delete(f.entriesByPath, child.Path)
		delete(f.contentByPath, child.Path)
		delete(f.metadataByPath, child.Path)
	}

	delete(f.childrenByPath, irodsPath)
	delete(f.entriesByPath, irodsPath)
	delete(f.metadataByPath, irodsPath)

	parentPath := path.Dir(irodsPath)
	if parentPath != "." && parentPath != "/" && parentPath != irodsPath {
		f.childrenByPath[parentPath] = filterChildEntry(f.childrenByPath[parentPath], irodsPath)
	}
}

func (f *testCatalogFileSystem) renameDirRecursive(srcPath string, destPath string) {
	entry := f.entriesByPath[srcPath]
	parentSrc := path.Dir(srcPath)
	parentDest := path.Dir(destPath)

	entry.Path = destPath
	entry.Name = path.Base(destPath)
	f.entriesByPath[destPath] = entry
	delete(f.entriesByPath, srcPath)

	children := f.childrenByPath[srcPath]
	delete(f.childrenByPath, srcPath)
	f.childrenByPath[destPath] = children

	if metas, ok := f.metadataByPath[srcPath]; ok {
		f.metadataByPath[destPath] = metas
		delete(f.metadataByPath, srcPath)
	}

	f.childrenByPath[parentSrc] = filterChildEntry(f.childrenByPath[parentSrc], srcPath)
	f.childrenByPath[parentDest] = append(f.childrenByPath[parentDest], entry)

	for _, child := range children {
		if child == nil {
			continue
		}
		childDest := path.Join(destPath, path.Base(child.Path))
		if child.IsDir() {
			f.renameDirRecursive(child.Path, childDest)
			continue
		}
		f.renameFileWithinDir(child.Path, childDest)
	}
}

func (f *testCatalogFileSystem) renameFileWithinDir(srcPath string, destPath string) {
	entry := f.entriesByPath[srcPath]
	entry.Path = destPath
	entry.Name = path.Base(destPath)
	f.entriesByPath[destPath] = entry
	delete(f.entriesByPath, srcPath)

	if data, ok := f.contentByPath[srcPath]; ok {
		f.contentByPath[destPath] = data
		delete(f.contentByPath, srcPath)
	}
	if metas, ok := f.metadataByPath[srcPath]; ok {
		f.metadataByPath[destPath] = metas
		delete(f.metadataByPath, srcPath)
	}
}

func filterChildEntry(entries []*irodsfs.Entry, targetPath string) []*irodsfs.Entry {
	filtered := entries[:0]
	for _, entry := range entries {
		if entry == nil || path.Clean(entry.Path) == targetPath {
			continue
		}
		filtered = append(filtered, entry)
	}
	return filtered
}

func userKey(username string, zone string) string {
	return strings.TrimSpace(zone) + "/" + strings.TrimSpace(username)
}

type testCatalogFileHandle struct {
	reader  *bytes.Reader
	writer  *bytes.Buffer
	onClose func([]byte)
}

func (f *testCatalogFileHandle) ReadAt(buffer []byte, offset int64) (int, error) {
	return f.reader.ReadAt(buffer, offset)
}

func (f *testCatalogFileHandle) Write(data []byte) (int, error) {
	if f.writer == nil {
		return 0, errors.New("file handle is not writable")
	}

	return f.writer.Write(data)
}

func (f *testCatalogFileHandle) Close() error {
	if f.onClose != nil && f.writer != nil {
		f.onClose(f.writer.Bytes())
	}
	return nil
}

func containsAll(s string, parts ...string) bool {
	for _, part := range parts {
		if !strings.Contains(s, part) {
			return false
		}
	}
	return true
}

func TestGetResourcesReturnsZoneResources(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/resource", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if body := rec.Body.String(); !containsAll(
		body,
		`"resources":[`,
		`"name":"demoResc"`,
		`"scope":"top"`,
		`"zone":"tempZone"`,
		`"location":"irods.example.org"`,
		`"self":{"href":"/api/v1/resource/demoResc","method":"GET"}`,
		`"self":{"href":"/api/v1/resource?scope=top","method":"GET"}`,
	) {
		t.Fatalf("unexpected response body: %q", body)
	}
}

func TestGetResourceReturnsResourceDetails(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/resource/demoResc", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if body := rec.Body.String(); !containsAll(body, `"resource":{"id":500`, `"name":"demoResc"`, `"self":{"href":"/api/v1/resource/demoResc","method":"GET"}`) {
		t.Fatalf("unexpected response body: %q", body)
	}
}

func TestGetResourcesAcceptsAllScope(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/resource?scope=all", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if body := rec.Body.String(); !containsAll(body, `"scope":"all"`, `"self":{"href":"/api/v1/resource?scope=all","method":"GET"}`) {
		t.Fatalf("unexpected response body: %q", body)
	}
}

func TestGetResourcesRejectsInvalidScope(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/resource?scope=bogus", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestGetUsersReturnsPrefixMatches(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/user?prefix=ali", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !containsAll(
		body,
		`"users":[`,
		`"name":"alice"`,
		`"name":"alicia"`,
		`"zone":"tempZone"`,
		`"type":"rodsuser"`,
		`"prefix":"ali"`,
		`"create":{"href":"/api/v1/user?zone=tempZone","method":"POST"}`,
		`"self":{"href":"/api/v1/user/alice?zone=tempZone","method":"GET"}`,
		`"update":{"href":"/api/v1/user/alice?zone=tempZone","method":"PUT"}`,
		`"update_type":{"href":"/api/v1/user/alice/type?zone=tempZone","method":"PUT"}`,
		`"update_password":{"href":"/api/v1/user/alice/password?zone=tempZone","method":"PUT"}`,
		`"delete":{"href":"/api/v1/user/alice?zone=tempZone","method":"DELETE"}`,
		`"avus":{"href":"/api/v1/user/alice/avu?zone=tempZone","method":"GET"}`,
		`"create_avu":{"href":"/api/v1/user/alice/avu?zone=tempZone","method":"POST"}`,
	) {
		t.Fatalf("unexpected response body: %q", body)
	}
	if strings.Contains(body, `"name":"bob"`) {
		t.Fatalf("expected prefix filter to exclude bob, got %q", body)
	}
}

func TestGetUsersRejectsShortPrefix(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/user?prefix=al", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestGetUserReturnsUserDetails(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/user/alice", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if body := rec.Body.String(); !containsAll(
		body,
		`"user":{"id":300`,
		`"name":"alice"`,
		`"zone":"tempZone"`,
		`"type":"rodsuser"`,
		`"self":{"href":"/api/v1/user/alice?zone=tempZone","method":"GET"}`,
		`"update_type":{"href":"/api/v1/user/alice/type?zone=tempZone","method":"PUT"}`,
		`"update_password":{"href":"/api/v1/user/alice/password?zone=tempZone","method":"PUT"}`,
	) {
		t.Fatalf("unexpected response body: %q", body)
	}
}

func TestGetUserMembershipSummaryIncludesGroupAdminsByDefault(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/user/membership-summary?zone=tempZone&limit=100", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !containsAll(body, `"name":"alice"`, `"type":"rodsuser"`, `"name":"groupadmin"`, `"type":"groupadmin"`) {
		t.Fatalf("expected default membership summary to include rodsuser and groupadmin users, got %q", body)
	}
}

func TestGetUserMembershipSummaryTypeFilterNarrowsUsers(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/user/membership-summary?zone=tempZone&type=rodsuser&limit=100", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !containsAll(body, `"name":"alice"`, `"type":"rodsuser"`) {
		t.Fatalf("expected rodsuser membership summary, got %q", body)
	}
	if strings.Contains(body, `"name":"groupadmin"`) {
		t.Fatalf("expected rodsuser filter to exclude groupadmin, got %q", body)
	}
}

func TestPutUserRequiresRodsAdmin(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/user/bob", strings.NewReader(`{"type":"rodsadmin"}`))
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", rec.Code)
	}
}

func TestPutUserRejectsGroupAdminTypeChange(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/user/groupadmin", strings.NewReader(`{"type":"rodsuser"}`))
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("groupadmin:secret")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestPutUserRejectsGroupAdminTypeChangeWithReconcileFlag(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/user/groupadmin?reconcile=true", strings.NewReader(`{"type":"rodsuser"}`))
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("groupadmin:secret")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestPutUserRejectsGroupAdminTypeChangeWithReconcileFalse(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/user/groupadmin?reconcile=false", strings.NewReader(`{"type":"rodsuser"}`))
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("groupadmin:secret")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestPutUserRejectsGroupAdminSelfPromotionToRodsAdmin(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/user/groupadmin", strings.NewReader(`{"type":"rodsadmin"}`))
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("groupadmin:secret")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestPutUserUpdatesUserAsRodsAdmin(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/user/bob", strings.NewReader(`{"type":"rodsadmin"}`))
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("rods:secret")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if body := rec.Body.String(); !containsAll(body, `"name":"bob"`, `"type":"rodsadmin"`) {
		t.Fatalf("unexpected response body: %q", body)
	}
}

func TestPutUserUpdatesUserToGroupAdminAsRodsAdmin(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/user/bob", strings.NewReader(`{"type":"groupadmin"}`))
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("rods:secret")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if body := rec.Body.String(); !containsAll(body, `"name":"bob"`, `"type":"groupadmin"`) {
		t.Fatalf("unexpected response body: %q", body)
	}
}

func TestPutUserTypeUpdatesUserAsRodsAdmin(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/user/bob/type", strings.NewReader(`{"type":"groupadmin"}`))
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("rods:secret")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if body := rec.Body.String(); !containsAll(body, `"name":"bob"`, `"type":"groupadmin"`) {
		t.Fatalf("unexpected response body: %q", body)
	}
}

func TestPutUserTypeRejectsMissingType(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/user/bob/type", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("rods:secret")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
	if body := rec.Body.String(); !containsAll(body, `"message":"user type update validation failed"`, `"type":"type is required"`) {
		t.Fatalf("unexpected response body: %q", body)
	}
}

func TestPutUserTypeRejectsPasswordField(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/user/bob/type", strings.NewReader(`{"type":"groupadmin","password":"new-secret"}`))
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("rods:secret")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
	if body := rec.Body.String(); !containsAll(body, `"message":"request body must be valid JSON with only supported fields"`) {
		t.Fatalf("unexpected response body: %q", body)
	}
}

func TestPutUserPasswordUpdatesUserAsRodsAdmin(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/user/bob/password", strings.NewReader(`{"password":"new-secret"}`))
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("rods:secret")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if body := rec.Body.String(); !containsAll(body, `"name":"bob"`) {
		t.Fatalf("unexpected response body: %q", body)
	}
}

func TestPutUserPasswordRejectsMissingPassword(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/user/bob/password", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("rods:secret")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
	if body := rec.Body.String(); !containsAll(body, `"message":"user password update validation failed"`, `"password":"password is required"`) {
		t.Fatalf("unexpected response body: %q", body)
	}
}

func TestPutUserPasswordRejectsTypeField(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/user/bob/password", strings.NewReader(`{"password":"new-secret","type":"groupadmin"}`))
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("rods:secret")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
	if body := rec.Body.String(); !containsAll(body, `"message":"request body must be valid JSON with only supported fields"`) {
		t.Fatalf("unexpected response body: %q", body)
	}
}

func TestPutUserRejectsMixedTypeAndPassword(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/user/bob", strings.NewReader(`{"type":"rodsadmin","password":"new-secret"}`))
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("rods:secret")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
	if body := rec.Body.String(); !containsAll(body, `"message":"user update validation failed"`, `"request":"type and password updates must use separate routes"`) {
		t.Fatalf("unexpected response body: %q", body)
	}
}

func TestPutUserReconcileCreatesMissingUserAsRodsAdmin(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/user/charlie?reconcile=true", strings.NewReader(`{"type":"rodsuser"}`))
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("rods:secret")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if body := rec.Body.String(); !containsAll(body, `"name":"charlie"`, `"type":"rodsuser"`) {
		t.Fatalf("unexpected response body: %q", body)
	}
}

func TestPutUserReconcileMissingWithoutTypeReturnsNotFound(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/user/charlie?reconcile=true", strings.NewReader(`{"password":"secret"}`))
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("rods:secret")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestPostUserRequiresRodsAdminOrGroupAdmin(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/user", strings.NewReader(`{"name":"charlie","type":"rodsuser"}`))
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", rec.Code)
	}
}

func TestPostUserCreatesRodsUserAsGroupAdmin(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/user", strings.NewReader(`{"name":"charlie","type":"rodsuser","password":"initial-pass"}`))
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("groupadmin:secret")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}
	if body := rec.Body.String(); !containsAll(body, `"name":"charlie"`, `"type":"rodsuser"`) {
		t.Fatalf("unexpected response body: %q", body)
	}
}

func TestPostUserRejectsProtectedTypeAsGroupAdmin(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/user", strings.NewReader(`{"name":"charlie","type":"groupadmin"}`))
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("groupadmin:secret")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestPostUserReconcileRejectsGroupAdmin(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/user?reconcile=true", strings.NewReader(`{"name":"charlie","type":"rodsuser"}`))
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("groupadmin:secret")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestPostUserDuplicateStrictConflicts(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/user", strings.NewReader(`{"name":"alice","type":"rodsuser"}`))
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("rods:secret")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", rec.Code)
	}
}

func TestPostUserReconcileExistingAsRodsAdmin(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/user?reconcile=true", strings.NewReader(`{"name":"alice","type":"rodsuser"}`))
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("rods:secret")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if body := rec.Body.String(); !containsAll(body, `"name":"alice"`, `"type":"rodsuser"`) {
		t.Fatalf("unexpected response body: %q", body)
	}
}

func TestPostUserReconcileExistingTypeMismatchConflicts(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/user?reconcile=true", strings.NewReader(`{"name":"alice","type":"rodsadmin"}`))
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("rods:secret")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", rec.Code)
	}
}

func TestDeleteUserRequiresRodsAdmin(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/user/bob", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", rec.Code)
	}
}

func TestDeleteUserRemovesUserAsRodsAdmin(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/user/bob", nil)
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("rods:secret")))
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", rec.Code)
	}
}

func TestDeleteUserRejectsGroupAdmin(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/user/bob", nil)
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("groupadmin:secret")))
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestDeleteUserReconcileMissingAsRodsAdmin(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/user/missing-user?reconcile=true", nil)
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("rods:secret")))
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", rec.Code)
	}
}

func TestUserMutationRejectsInvalidReconcileFlag(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/user?reconcile=sometimes", strings.NewReader(`{"name":"charlie","type":"rodsuser"}`))
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("groupadmin:secret")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
	if body := rec.Body.String(); !containsAll(body, `"code":"invalid_request"`, `"message":"reconcile must be true or false"`) {
		t.Fatalf("unexpected response body: %q", body)
	}
}

func TestGetUserGroupsReturnsPrefixMatches(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/usergroup?prefix=res", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !containsAll(
		body,
		`"groups":[`,
		`"name":"research-team"`,
		`"type":"rodsgroup"`,
		`"prefix":"res"`,
		`"create":{"href":"/api/v1/usergroup?zone=tempZone","method":"POST"}`,
		`"self":{"href":"/api/v1/usergroup/research-team?zone=tempZone","method":"GET"}`,
		`"delete":{"href":"/api/v1/usergroup/research-team?zone=tempZone","method":"DELETE"}`,
		`"add_member":{"href":"/api/v1/usergroup/research-team/member?zone=tempZone","method":"POST"}`,
		`"avus":{"href":"/api/v1/usergroup/research-team/avu?zone=tempZone","method":"GET"}`,
		`"create_avu":{"href":"/api/v1/usergroup/research-team/avu?zone=tempZone","method":"POST"}`,
	) {
		t.Fatalf("unexpected response body: %q", body)
	}
}

func TestGetUserGroupsRejectsShortPrefix(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/usergroup?prefix=re", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestGetUserGroupReturnsMembersAndLinks(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/usergroup/research-team", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !containsAll(
		body,
		`"group":{"id":305`,
		`"name":"research-team"`,
		`"type":"rodsgroup"`,
		`"members":[{"id":300,"name":"alice","zone":"tempZone","type":"rodsuser"`,
		`"self":{"href":"/api/v1/user/alice?zone=tempZone","method":"GET"}`,
		`"remove_from_group":{"href":"/api/v1/usergroup/research-team/member/alice?zone=tempZone","method":"DELETE"}`,
		`"add_member":{"href":"/api/v1/usergroup/research-team/member?zone=tempZone","method":"POST"}`,
		`"avus":{"href":"/api/v1/usergroup/research-team/avu?zone=tempZone","method":"GET"}`,
		`"create_avu":{"href":"/api/v1/usergroup/research-team/avu?zone=tempZone","method":"POST"}`,
	) {
		t.Fatalf("unexpected response body: %q", body)
	}
}

func TestPostUserGroupRequiresAdminOrGroupAdmin(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/usergroup", strings.NewReader(`{"name":"science"}`))
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", rec.Code)
	}
}

func TestPostUserGroupRejectsMissingName(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/usergroup", strings.NewReader(`{"name":" "}`))
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("groupadmin:secret")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
	if body := rec.Body.String(); !containsAll(body, `"code":"invalid_request"`, `"message":"user group create validation failed"`, `"fields":{"name":"name is required"}`) {
		t.Fatalf("unexpected response body: %q", body)
	}
}

func TestPostUserGroupCreatesAsGroupAdmin(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/usergroup", strings.NewReader(`{"name":"science"}`))
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("groupadmin:secret")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rec.Code)
	}
	if body := rec.Body.String(); !containsAll(body, `"name":"science"`, `"type":"rodsgroup"`) {
		t.Fatalf("unexpected response body: %q", body)
	}
}

func TestPostUserGroupDuplicateStrictConflicts(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/usergroup", strings.NewReader(`{"name":"research-team"}`))
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("groupadmin:secret")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", rec.Code)
	}
}

func TestPostUserGroupReconcileExistingAsGroupAdmin(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/usergroup?reconcile=true", strings.NewReader(`{"name":"research-team"}`))
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("groupadmin:secret")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if body := rec.Body.String(); !containsAll(body, `"name":"research-team"`, `"type":"rodsgroup"`) {
		t.Fatalf("unexpected response body: %q", body)
	}
}

func TestDeleteUserGroupRejectsGroupAdmin(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/usergroup/research-team", nil)
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("groupadmin:secret")))
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestDeleteUserGroupReconcileMissingAsRodsAdmin(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/usergroup/missing-team?reconcile=true", nil)
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("rods:secret")))
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", rec.Code)
	}
}

func TestDeleteUserGroupRequiresAdminOrGroupAdmin(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/usergroup/research-team", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", rec.Code)
	}
	if body := rec.Body.String(); !containsAll(body, `"code":"permission_denied"`) {
		t.Fatalf("unexpected response body: %q", body)
	}
}

func TestPostUserGroupMemberAddsUserAsGroupAdmin(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/usergroup/research-team/member", strings.NewReader(`{"user_name":"bob"}`))
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("groupadmin:secret")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if body := rec.Body.String(); !containsAll(body, `"name":"research-team"`, `"name":"bob"`, `"remove_from_group":{"href":"/api/v1/usergroup/research-team/member/bob?zone=tempZone","method":"DELETE"}`) {
		t.Fatalf("unexpected response body: %q", body)
	}
}

func TestPostUserGroupMemberReconcileExistingAsGroupAdmin(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/usergroup/research-team/member?reconcile=true", strings.NewReader(`{"user_name":"alice"}`))
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("groupadmin:secret")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if body := rec.Body.String(); !containsAll(body, `"name":"research-team"`, `"name":"alice"`) {
		t.Fatalf("unexpected response body: %q", body)
	}
}

func TestPostUserGroupMemberRequiresAdminOrGroupAdmin(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/usergroup/research-team/member", strings.NewReader(`{"user_name":"bob"}`))
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", rec.Code)
	}
	if body := rec.Body.String(); !containsAll(body, `"code":"permission_denied"`) {
		t.Fatalf("unexpected response body: %q", body)
	}
}

func TestPostUserGroupMemberRejectsMissingUserName(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/usergroup/research-team/member", strings.NewReader(`{"user_name":" "}`))
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("groupadmin:secret")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
	if body := rec.Body.String(); !containsAll(body, `"code":"invalid_request"`, `"message":"user group member validation failed"`, `"fields":{"user_name":"user_name is required"}`) {
		t.Fatalf("unexpected response body: %q", body)
	}
}

func TestDeleteUserGroupMemberRemovesUserAsGroupAdmin(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/usergroup/research-team/member/alice", nil)
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("groupadmin:secret")))
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if body := rec.Body.String(); strings.Contains(body, `"name":"alice"`) {
		t.Fatalf("expected alice to be removed, got %q", body)
	}
}

func TestDeleteUserGroupMemberReconcileMissingAsGroupAdmin(t *testing.T) {
	handler, filesystem := testHandlerWithConfig(t, nil)
	filesystem.addUserMetadata("research-team", "tempZone", usersyncext.AVUAttributeManaged, usersyncext.AVUValueTrue, "")

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/usergroup/research-team/member/bob?reconcile=true", nil)
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("groupadmin:secret")))
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if body := rec.Body.String(); !containsAll(body, `"name":"research-team"`, `"name":"alice"`) || strings.Contains(body, `"name":"bob"`) {
		t.Fatalf("unexpected response body: %q", body)
	}
}

func TestDeleteUserGroupMemberRequiresAdminOrGroupAdmin(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/usergroup/research-team/member/alice", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", rec.Code)
	}
	if body := rec.Body.String(); !containsAll(body, `"code":"permission_denied"`) {
		t.Fatalf("unexpected response body: %q", body)
	}
}

func TestUserGroupMutationRejectsInvalidReconcileFlag(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/usergroup?reconcile=sometimes", strings.NewReader(`{"name":"science"}`))
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("groupadmin:secret")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
	if body := rec.Body.String(); !containsAll(body, `"code":"invalid_request"`, `"message":"reconcile must be true or false"`) {
		t.Fatalf("unexpected response body: %q", body)
	}
}

func TestUserGroupHandlersRejectMissingPathParameters(t *testing.T) {
	handler := testHandler(t)

	tests := []struct {
		name       string
		method     string
		target     string
		body       string
		pathValues map[string]string
		handle     func(http.ResponseWriter, *http.Request)
		want       string
	}{
		{
			name:   "get group missing group_name",
			method: http.MethodGet,
			target: "/api/v1/usergroup/",
			handle: handler.getUserGroup,
			want:   `"message":"group_name path parameter is required"`,
		},
		{
			name:   "delete group missing group_name",
			method: http.MethodDelete,
			target: "/api/v1/usergroup/",
			handle: handler.deleteUserGroup,
			want:   `"message":"group_name path parameter is required"`,
		},
		{
			name:   "post member missing group_name",
			method: http.MethodPost,
			target: "/api/v1/usergroup//member",
			body:   `{"user_name":"alice"}`,
			handle: handler.postUserGroupMember,
			want:   `"message":"group_name path parameter is required"`,
		},
		{
			name:   "delete member missing group_name",
			method: http.MethodDelete,
			target: "/api/v1/usergroup//member/alice",
			handle: handler.deleteUserGroupMember,
			want:   `"message":"group_name path parameter is required"`,
		},
		{
			name:   "delete member missing user_name",
			method: http.MethodDelete,
			target: "/api/v1/usergroup/research-team/member/",
			pathValues: map[string]string{
				"group_name": "research-team",
			},
			handle: handler.deleteUserGroupMember,
			want:   `"message":"user_name path parameter is required"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.target, strings.NewReader(tt.body))
			if tt.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}
			for key, value := range tt.pathValues {
				req.SetPathValue(key, value)
			}
			rec := httptest.NewRecorder()

			tt.handle(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d", rec.Code)
			}
			if body := rec.Body.String(); !containsAll(body, `"code":"invalid_request"`, tt.want) {
				t.Fatalf("unexpected response body: %q", body)
			}
		})
	}
}
