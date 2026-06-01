package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/michael-conway/irods-go-rest/internal/auth"
)

func TestParseIRODSTicketBearer(t *testing.T) {
	ticket, ok := parseIRODSTicketBearer("irods-ticket:ticket123")
	if !ok {
		t.Fatal("expected ticket bearer token to parse")
	}
	if ticket != "ticket123" {
		t.Fatalf("expected parsed ticket ticket123, got %q", ticket)
	}
}

func TestParseIRODSTicketBearerRejectsEmptyTicket(t *testing.T) {
	if ticket, ok := parseIRODSTicketBearer("irods-ticket:   "); ok || ticket != "" {
		t.Fatalf("expected empty ticket bearer token to be rejected, got %q", ticket)
	}
}

func TestParseIRODSTicketBearerRejectsNonTicketToken(t *testing.T) {
	if ticket, ok := parseIRODSTicketBearer("token123"); ok || ticket != "" {
		t.Fatalf("expected non-ticket bearer token to be rejected, got %q", ticket)
	}
}

func TestRequireDownloadBearerAcceptsTicketIDQueryWithoutAuthorization(t *testing.T) {
	handler := &Handler{}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/path/contents?irods_path=/tempZone/home/test1/file.txt&ticket_id=ticket-query-123", nil)
	rec := httptest.NewRecorder()

	called := false
	handler.requireDownloadBearer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		ticket, ok := auth.TicketFromContext(r.Context())
		if !ok || ticket != "ticket-query-123" {
			t.Fatalf("expected ticket from query context, got %q", ticket)
		}
		w.WriteHeader(http.StatusOK)
	})).ServeHTTP(rec, req)

	if !called {
		t.Fatal("expected wrapped handler to be called")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestRequireDownloadBearerPrefersTicketIDQueryOverBearerTicket(t *testing.T) {
	handler := &Handler{}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/path/contents?irods_path=/tempZone/home/test1/file.txt&ticket_id=ticket-query-123", nil)
	req.Header.Set("Authorization", "Bearer irods-ticket:ticket-header-456")
	rec := httptest.NewRecorder()

	handler.requireDownloadBearer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ticket, ok := auth.TicketFromContext(r.Context())
		if !ok || ticket != "ticket-query-123" {
			t.Fatalf("expected query ticket to take precedence, got %q", ticket)
		}
		w.WriteHeader(http.StatusOK)
	})).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestRequireBearerExpectedAuthFailureOmitsStackTrace(t *testing.T) {
	handler := &Handler{}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/path", nil)
	rec := httptest.NewRecorder()

	record := captureAuthLogRecord(t, func() {
		handler.requireBearer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})).ServeHTTP(rec, req)
	})

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
	if _, ok := record["stack_trace"]; ok {
		t.Fatalf("expected no stack_trace for expected auth failure, got %v", record["stack_trace"])
	}
	if got := stringField(record, "level"); got != "WARN" {
		t.Fatalf("expected WARN level for expected auth failure, got %q", got)
	}
}

func TestRequireBearerInternalAuthFailureIncludesStackTrace(t *testing.T) {
	handler := &Handler{
		verifier: staticTokenVerifier{err: auth.ErrNotConfigured},
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/path", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	record := captureAuthLogRecord(t, func() {
		handler.requireBearer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})).ServeHTTP(rec, req)
	})

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 for auth not configured, got %d", rec.Code)
	}
	if _, ok := record["stack_trace"]; !ok {
		t.Fatal("expected stack_trace for internal auth failure")
	}
	if got := stringField(record, "level"); got != "ERROR" {
		t.Fatalf("expected ERROR level for internal auth failure, got %q", got)
	}
}

type staticTokenVerifier struct {
	principal auth.Principal
	err       error
}

func (s staticTokenVerifier) VerifyToken(_ context.Context, _ string) (auth.Principal, error) {
	if s.err != nil {
		return auth.Principal{}, s.err
	}
	return s.principal, nil
}

func captureAuthLogRecord(t *testing.T, run func()) map[string]any {
	t.Helper()

	previous := slog.Default()
	var logBuffer bytes.Buffer
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logBuffer, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))
	defer slog.SetDefault(previous)

	run()

	lines := strings.Split(strings.TrimSpace(logBuffer.String()), "\n")
	if len(lines) == 0 || strings.TrimSpace(lines[len(lines)-1]) == "" {
		t.Fatal("expected auth middleware log output")
	}

	record := map[string]any{}
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &record); err != nil {
		t.Fatalf("decode auth middleware log json: %v", err)
	}
	return record
}
