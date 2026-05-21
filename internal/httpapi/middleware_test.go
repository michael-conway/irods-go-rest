package httpapi

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/michael-conway/irods-go-rest/internal/requestctx"
)

func TestRequestLoggerEmitsStructuredFields(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/path", func(w http.ResponseWriter, r *http.Request) {
		setRequestAuthMetadata(w, "basic", "alice")
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	})

	record := captureRequestLogRecord(t, requestLogger(mux), httptest.NewRequest(http.MethodGet, "/api/v1/path?irods_path=/tempZone/home/test1/file.txt", nil))

	if got := stringField(record, "method"); got != http.MethodGet {
		t.Fatalf("expected method GET, got %q", got)
	}
	if got := stringField(record, "route"); got != "GET /api/v1/path" {
		t.Fatalf("expected route GET /api/v1/path, got %q", got)
	}
	if got := intField(record, "status"); got != http.StatusOK {
		t.Fatalf("expected status 200, got %d", got)
	}
	if got := stringField(record, "auth_mode"); got != "basic" {
		t.Fatalf("expected auth_mode basic, got %q", got)
	}
	if got := stringField(record, "principal"); got != "alice" {
		t.Fatalf("expected principal alice, got %q", got)
	}
	if got := stringField(record, "irods_path"); got != "/tempZone/home/test1/file.txt" {
		t.Fatalf("expected irods_path in request log, got %q", got)
	}
}

func TestRequestLoggerEmitsErrorCodeAndClass(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/path", func(w http.ResponseWriter, r *http.Request) {
		setRequestAuthMetadata(w, "bearer", "alice")
		writeError(w, http.StatusForbidden, "permission_denied", "denied")
	})

	record := captureRequestLogRecord(t, requestLogger(mux), httptest.NewRequest(http.MethodGet, "/api/v1/path", nil))

	if got := intField(record, "status"); got != http.StatusForbidden {
		t.Fatalf("expected status 403, got %d", got)
	}
	if got := stringField(record, "error_code"); got != "permission_denied" {
		t.Fatalf("expected error_code permission_denied, got %q", got)
	}
	if got := stringField(record, "error_class"); got != "permission_denied" {
		t.Fatalf("expected error_class permission_denied, got %q", got)
	}
}

func TestRequestLoggerCapturesAuthFailureMetadata(t *testing.T) {
	handler := &Handler{}
	mux := http.NewServeMux()
	mux.Handle("GET /api/v1/path", handler.requireBearer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	})))

	record := captureRequestLogRecord(t, requestLogger(mux), httptest.NewRequest(http.MethodGet, "/api/v1/path", nil))

	if got := intField(record, "status"); got != http.StatusUnauthorized {
		t.Fatalf("expected status 401, got %d", got)
	}
	if got := stringField(record, "auth_mode"); got != "none" {
		t.Fatalf("expected auth_mode none, got %q", got)
	}
	if got := stringField(record, "error_code"); got != "missing_authorization" {
		t.Fatalf("expected error_code missing_authorization, got %q", got)
	}
	if got := stringField(record, "error_class"); got != "auth_error" {
		t.Fatalf("expected error_class auth_error, got %q", got)
	}
}

func TestRequestLoggerPropagatesRequestAuditMetadata(t *testing.T) {
	handler := &Handler{verifier: stubAuthService{}}
	mux := http.NewServeMux()
	mux.Handle("GET /api/v1/path", handler.requireBearer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		metadata, ok := requestctx.MetadataFromContext(r.Context())
		if !ok {
			t.Fatal("expected request metadata in context")
		}
		if metadata.RequestID != "req-123" || metadata.Source != "irods-keycloak-admin" || metadata.Actor != "sync-apply" || metadata.IdempotencyKey != "idem-123" {
			t.Fatalf("unexpected request metadata: %+v", metadata)
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	})))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/path", nil)
	req.Header.Set("Authorization", "Bearer token123")
	req.Header.Set(requestIDHeader, "req-123")
	req.Header.Set(requestSourceHeader, "irods-keycloak-admin")
	req.Header.Set(requestActorHeader, "sync-apply")
	req.Header.Set(idempotencyKeyHeader, "idem-123")

	record, recorder := captureRequestLogRecordAndRecorder(t, requestLogger(mux), req)

	if got := recorder.Header().Get(requestIDHeader); got != "req-123" {
		t.Fatalf("expected response request id req-123, got %q", got)
	}
	if got := stringField(record, "request_id"); got != "req-123" {
		t.Fatalf("expected request_id req-123, got %q", got)
	}
	if got := stringField(record, "request_source"); got != "irods-keycloak-admin" {
		t.Fatalf("expected request_source irods-keycloak-admin, got %q", got)
	}
	if got := stringField(record, "request_actor"); got != "sync-apply" {
		t.Fatalf("expected request_actor sync-apply, got %q", got)
	}
	if got := stringField(record, "idempotency_key"); got != "idem-123" {
		t.Fatalf("expected idempotency_key idem-123, got %q", got)
	}
	if got := stringField(record, "auth_subject"); got != "user-123" {
		t.Fatalf("expected auth_subject user-123, got %q", got)
	}
	if got := stringField(record, "auth_client_id"); got != "irods-go-rest" {
		t.Fatalf("expected auth_client_id irods-go-rest, got %q", got)
	}
	if got := stringField(record, "auth_scope"); got != "openid profile" {
		t.Fatalf("expected auth_scope openid profile, got %q", got)
	}
	if got := stringField(record, "auth_audience"); got != "irods-go-rest" {
		t.Fatalf("expected auth_audience irods-go-rest, got %q", got)
	}
}

func captureRequestLogRecord(t *testing.T, handler http.Handler, req *http.Request) map[string]any {
	t.Helper()
	record, _ := captureRequestLogRecordAndRecorder(t, handler, req)
	return record
}

func captureRequestLogRecordAndRecorder(t *testing.T, handler http.Handler, req *http.Request) (map[string]any, *httptest.ResponseRecorder) {
	t.Helper()

	previous := slog.Default()
	var logBuffer bytes.Buffer
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logBuffer, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))
	defer slog.SetDefault(previous)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	lines := strings.Split(strings.TrimSpace(logBuffer.String()), "\n")
	if len(lines) == 0 || strings.TrimSpace(lines[len(lines)-1]) == "" {
		t.Fatal("expected request logger output")
	}

	record := map[string]any{}
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &record); err != nil {
		t.Fatalf("decode request log json: %v", err)
	}
	return record, recorder
}

func stringField(record map[string]any, key string) string {
	value, ok := record[key]
	if !ok || value == nil {
		return ""
	}
	if typed, ok := value.(string); ok {
		return typed
	}
	return ""
}

func intField(record map[string]any, key string) int {
	value, ok := record[key]
	if !ok || value == nil {
		return 0
	}
	switch typed := value.(type) {
	case float64:
		return int(typed)
	case int:
		return typed
	default:
		return 0
	}
}
