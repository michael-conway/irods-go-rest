package httpapi

import (
	"net/http"
	"net/http/httptest"
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
