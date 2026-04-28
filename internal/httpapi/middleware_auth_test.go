package httpapi

import "testing"

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
