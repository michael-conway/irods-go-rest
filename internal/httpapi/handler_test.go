package httpapi

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/michael-conway/irods-go-rest/internal/auth"
	"github.com/michael-conway/irods-go-rest/internal/config"
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
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/web/login", nil)
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusFound {
		t.Fatalf("expected 302, got %d", rec.Code)
	}
}

func TestWebHomeDisplaysBearerToken(t *testing.T) {
	handler := testHandler(t)

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
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/web/callback?code=code123&state=state123", nil)
	req.AddCookie(&http.Cookie{Name: authStateCookieName, Value: "state123"})
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusFound {
		t.Fatalf("expected 302, got %d", rec.Code)
	}
}

func TestWebCallbackSurfacesOAuthError(t *testing.T) {
	handler := testHandler(t)

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

func TestAPIRequiresBearerToken(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/path?irods_path=/tempZone/home/test1/file.txt", nil)
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
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

	if body := rec.Body.String(); !containsAll(body, `"parent":{"irods_path":"/tempZone/home/test1"`, `"/api/v1/path?irods_path=%2FtempZone%2Fhome%2Ftest1"`) {
		t.Fatalf("expected parent link in response body: %q", body)
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

	if body := rec.Body.String(); !containsAll(body, `"/tempZone/home/test1/file.txt"`, `"source":"scaffold"`) {
		t.Fatalf("unexpected response body: %q", body)
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

	if body := rec.Body.String(); !containsAll(body, `"parent":{"irods_path":"/tempZone/home/test1"`) {
		t.Fatalf("expected parent link in collection response body: %q", body)
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

	if body := rec.Body.String(); !containsAll(body, `"children"`, `"kind":"data_object"`, `"kind":"collection"`) {
		t.Fatalf("unexpected children response body: %q", body)
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

	if body := rec.Body.String(); !containsAll(body, `"parent":{"irods_path":"/tempZone/home/test1/project"`) {
		t.Fatalf("expected child parent links in response body: %q", body)
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

	if body := rec.Body.String(); body != "demo content for /tempZone/home/test1/file.txt" {
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

	if body := rec.Body.String(); body != "conten" {
		t.Fatalf("unexpected ranged content body %q", body)
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
		Active:   true,
	}, nil
}

func testHandler(t *testing.T) *Handler {
	t.Helper()

	cfg, err := config.ReadRestConfig("rest-config", "yaml", []string{"../config"})
	if err != nil {
		t.Fatalf("read rest config: %v", err)
	}
	return NewHandler(*cfg, restservice.NewPathService(irods.NewCatalogService(*cfg)), stubAuthService{}, stubAuthService{}, auth.NewSessionStore())
}

func containsAll(s string, parts ...string) bool {
	for _, part := range parts {
		if !strings.Contains(s, part) {
			return false
		}
	}
	return true
}
