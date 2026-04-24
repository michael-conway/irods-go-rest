package app

import "testing"

func TestListenAddrFromURL(t *testing.T) {
	if addr := listenAddr("http://localhost:8080"); addr != "localhost:8080" {
		t.Fatalf("expected localhost:8080, got %q", addr)
	}
}

func TestListenAddrFromHostPort(t *testing.T) {
	if addr := listenAddr("localhost:8080"); addr != "localhost:8080" {
		t.Fatalf("expected localhost:8080, got %q", addr)
	}
}
