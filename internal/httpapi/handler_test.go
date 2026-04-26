package httpapi

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
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
	if body := rec.Body.String(); !containsAll(body, `"display_size":"128 B"`, `"created_at":"2023-11-14T22:13:20Z"`, `"updated_at":"2023-11-14T22:13:20Z"`) {
		t.Fatalf("expected display size and timestamps in response body: %q", body)
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
	factory := func(_ *irodstypes.IRODSAccount, _ string) (irods.CatalogFileSystem, error) {
		return newTestCatalogFileSystem(), nil
	}

	return NewHandler(*cfg, restservice.NewPathService(irods.NewCatalogServiceWithFactory(*cfg, factory)), stubAuthService{}, stubAuthService{}, auth.NewSessionStore())
}

type testCatalogFileSystem struct {
	entriesByPath  map[string]*irodsfs.Entry
	childrenByPath map[string][]*irodsfs.Entry
	metadataByPath map[string][]*irodstypes.IRODSMeta
	contentByPath  map[string][]byte
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
		Path:              "/tempZone/home/test1/file.txt",
		Size:              128,
		CheckSumAlgorithm: irodstypes.ChecksumAlgorithmSHA256,
		CheckSum:          []byte("abc123"),
		IRODSReplicas: []irodstypes.IRODSReplica{{
			ResourceName: "demoResc",
		}},
		CreateTime: now,
		ModifyTime: now,
	}
	child := &irodsfs.Entry{
		ID:                102,
		Type:              irodsfs.FileEntry,
		Name:              "child.txt",
		Path:              "/tempZone/home/test1/project/child.txt",
		Size:              64,
		CheckSumAlgorithm: irodstypes.ChecksumAlgorithmSHA256,
		CheckSum:          []byte("childsum"),
		CreateTime:        now,
		ModifyTime:        now,
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
				Name:  "source",
				Value: "test",
			}},
			file.Path: {{
				Name:  "source",
				Value: "test",
			}},
		},
		contentByPath: map[string][]byte{
			file.Path:  []byte("hello content payload"),
			child.Path: []byte("child content payload"),
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

func (f *testCatalogFileSystem) OpenFile(irodsPath string, _ string, _ string) (irods.CatalogFileHandle, error) {
	data, ok := f.contentByPath[irodsPath]
	if !ok {
		return nil, errors.New("not found")
	}

	return &testCatalogFileHandle{reader: bytes.NewReader(data)}, nil
}

func (f *testCatalogFileSystem) Release() {}

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
