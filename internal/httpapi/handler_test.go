package httpapi

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"strings"
	"testing"
	"time"

	irodsfs "github.com/cyverse/go-irodsclient/fs"
	irodscommon "github.com/cyverse/go-irodsclient/irods/common"
	irodstypes "github.com/cyverse/go-irodsclient/irods/types"
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

	if body := rec.Body.String(); !containsAll(
		body,
		`"path_segments"`,
		`"display_name":"tempZone"`,
		`"display_name":"file.txt"`,
		`"/api/v1/path?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt"`,
	) {
		t.Fatalf("expected path segments in response body: %q", body)
	}

	if body := rec.Body.String(); !containsAll(body, `"parent":{"irods_path":"/tempZone/home/test1"`, `"/api/v1/path?irods_path=%2FtempZone%2Fhome%2Ftest1"`) {
		t.Fatalf("expected parent link in response body: %q", body)
	}
}

func TestGetServerInfo(t *testing.T) {
	handler := testHandler(t)

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
		`"avus":{"href":"/api/v1/path/avu?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"GET"}`,
		`"replicas":{"href":"/api/v1/path/replicas?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"GET"}`,
		`"create_avu":{"href":"/api/v1/path/avu?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"POST"}`,
		`"resource_link":{"href":"/api/v1/resource/demoResc","method":"GET"}`,
		`"cmd_cue":{"operation":"get","gocmd":"gocmd get '/tempZone/home/test1/file.txt' \u003cDESTINATION_PATH\u003e","icommand":"iget '/tempZone/home/test1/file.txt' \u003cDESTINATION_PATH\u003e"}`,
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
		`"cmd_cue":{"operation":"put","gocmd":"gocmd put \u003cLOCAL_PATH\u003e '/tempZone/home/test1/project'","icommand":"iput \u003cLOCAL_PATH\u003e '/tempZone/home/test1/project'"}`,
		`"resources":{"href":"/api/v1/resource","method":"GET"}`,
		`"create_child_collection":{"href":"/api/v1/path?irods_path=%2FtempZone%2Fhome%2Ftest1%2Fproject","method":"POST"}`,
		`"create_child_data_object":{"href":"/api/v1/path?irods_path=%2FtempZone%2Fhome%2Ftest1%2Fproject","method":"POST"}`,
		`"set_inheritance":{"href":"/api/v1/path/acl/inheritance?irods_path=%2FtempZone%2Fhome%2Ftest1%2Fproject","method":"PUT"}`,
		`"delete_inheritance":{"href":"/api/v1/path/acl/inheritance?irods_path=%2FtempZone%2Fhome%2Ftest1%2Fproject","method":"DELETE"}`,
	) {
		t.Fatalf("expected path segments in collection response body: %q", body)
	}

	if body := rec.Body.String(); !containsAll(body, `"parent":{"irods_path":"/tempZone/home/test1"`) {
		t.Fatalf("expected parent link in collection response body: %q", body)
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

	if body := rec.Body.String(); !containsAll(body, `"code":"conflict"`, `force=true`) {
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
		`"replicas":[`,
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
		`"links":{"avus":{"href":"/api/v1/path/avu?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"GET"},"acls":{"href":"/api/v1/path/acl?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"GET"},"create_avu":{"href":"/api/v1/path/avu?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"POST"},"create_ticket":{"href":"/api/v1/path/ticket?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"POST"},"resources":{"href":"/api/v1/resource","method":"GET"}}`,
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
	filesystem := newTestCatalogFileSystem()
	factory := func(_ *irodstypes.IRODSAccount, _ string) (irods.CatalogFileSystem, error) {
		return filesystem, nil
	}

	return NewHandler(
		*cfg,
		restservice.NewPathService(irods.NewCatalogServiceWithFactory(*cfg, factory)),
		restservice.NewServerInfoService(irods.NewServerInfoServiceWithFactory(*cfg, factory)),
		restservice.NewResourceService(irods.NewResourceServiceWithFactory(*cfg, factory)),
		restservice.NewUserService(irods.NewUserServiceWithFactory(*cfg, factory)),
		restservice.NewUserGroupService(irods.NewUserGroupServiceWithFactory(*cfg, factory)),
		restservice.NewTicketService(irods.NewTicketServiceWithFactory(*cfg, factory)),
		stubAuthService{},
		stubAuthService{},
		auth.NewSessionStore(),
	)
}

type testCatalogFileSystem struct {
	entriesByPath  map[string]*irodsfs.Entry
	childrenByPath map[string][]*irodsfs.Entry
	metadataByPath map[string][]*irodstypes.IRODSMeta
	aclByPath      map[string][]*irodstypes.IRODSAccess
	inheritByPath  map[string]bool
	contentByPath  map[string][]byte
	ticketsByName  map[string]*irodstypes.IRODSTicket
	resources      []*irodstypes.IRODSResource
	serverVersion  *irodstypes.IRODSVersion
	usersByKey     map[string]*irodstypes.IRODSUser
	groupMembers   map[string][]string
}

func newTestCatalogFileSystem() *testCatalogFileSystem {
	now := time.Unix(1_700_000_000, 0)

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
			project.Path: project,
			file.Path:    file,
			child.Path:   child,
			nested.Path:  nested,
		},
		childrenByPath: map[string][]*irodsfs.Entry{
			project.Path: {child, nested},
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

func (f *testCatalogFileSystem) MakeDir(irodsPath string, _ bool) error {
	parentPath := path.Dir(path.Clean(irodsPath))
	if parentPath != "." && parentPath != "/" {
		if _, ok := f.entriesByPath[parentPath]; !ok {
			return errors.New("not found")
		}
	}

	now := time.Unix(1_700_000_002, 0)
	entry := &irodsfs.Entry{
		ID:         int64(len(f.entriesByPath) + 200),
		Type:       irodsfs.DirectoryEntry,
		Name:       path.Base(irodsPath),
		Path:       path.Clean(irodsPath),
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
	parentPath := path.Dir(path.Clean(irodsPath))
	if _, ok := f.entriesByPath[parentPath]; !ok {
		return nil, errors.New("not found")
	}

	now := time.Unix(1_700_000_002, 0)
	entry := &irodsfs.Entry{
		ID:         int64(len(f.entriesByPath) + 200),
		Type:       irodsfs.FileEntry,
		Name:       path.Base(irodsPath),
		Path:       path.Clean(irodsPath),
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

func (f *testCatalogFileSystem) TrimDataObject(irodsPath string, resource string, minCopies int, _ int) error {
	resource = strings.TrimSpace(resource)
	if resource == "" {
		return errors.New("resource is required")
	}

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

func (f *testCatalogFileSystem) Release() {}

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
		`"delete":{"href":"/api/v1/user/alice?zone=tempZone","method":"DELETE"}`,
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
	) {
		t.Fatalf("unexpected response body: %q", body)
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

func TestPostUserRequiresAdminOrGroupAdmin(t *testing.T) {
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

func TestPostUserCreatesUserAsGroupAdmin(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/user", strings.NewReader(`{"name":"charlie","type":"rodsuser"}`))
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("groupadmin:secret")))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rec.Code)
	}
	if body := rec.Body.String(); !containsAll(body, `"name":"charlie"`, `"type":"rodsuser"`) {
		t.Fatalf("unexpected response body: %q", body)
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

func TestDeleteUserRemovesUserAsGroupAdmin(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/user/bob", nil)
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("groupadmin:secret")))
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", rec.Code)
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

func TestDeleteUserGroupRemovesAsGroupAdmin(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/usergroup/research-team", nil)
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("groupadmin:secret")))
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", rec.Code)
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
