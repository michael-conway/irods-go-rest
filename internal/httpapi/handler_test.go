package httpapi

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
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
		`"create_avu":{"href":"/api/v1/path/avu?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"POST"}`,
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
	) {
		t.Fatalf("expected path segments in collection response body: %q", body)
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
		`"links":{"avus":{"href":"/api/v1/path/avu?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"GET"},"create_avu":{"href":"/api/v1/path/avu?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"POST"},"create_ticket":{"href":"/api/v1/path/ticket?irods_path=%2FtempZone%2Fhome%2Ftest1%2Ffile.txt","method":"POST"}}`,
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
	if body := rec.Body.String(); !containsAll(body, `"tickets":[`, `"name":"ticket-existing"`, `"self":{"href":"/api/v1/ticket/ticket-existing","method":"GET"}`, `"create":{"href":"/api/v1/ticket","method":"POST"}`) {
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
	filesystem := newTestCatalogFileSystem()
	factory := func(_ *irodstypes.IRODSAccount, _ string) (irods.CatalogFileSystem, error) {
		return filesystem, nil
	}

	return NewHandler(
		*cfg,
		restservice.NewPathService(irods.NewCatalogServiceWithFactory(*cfg, factory)),
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
	contentByPath  map[string][]byte
	ticketsByName  map[string]*irodstypes.IRODSTicket
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

func (f *testCatalogFileSystem) OpenFile(irodsPath string, _ string, _ string) (irods.CatalogFileHandle, error) {
	data, ok := f.contentByPath[irodsPath]
	if !ok {
		return nil, errors.New("not found")
	}

	return &testCatalogFileHandle{reader: bytes.NewReader(data)}, nil
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

type testCatalogFileHandle struct {
	reader *bytes.Reader
}

func (f *testCatalogFileHandle) ReadAt(buffer []byte, offset int64) (int, error) {
	return f.reader.ReadAt(buffer, offset)
}

func (f *testCatalogFileHandle) Close() error {
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
