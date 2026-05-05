package app

import (
	"testing"

	"github.com/michael-conway/irods-go-rest/internal/config"
)

func TestPublicURLListenAddrFromURL(t *testing.T) {
	if addr := publicURLListenAddr("http://localhost:8080"); addr != "localhost:8080" {
		t.Fatalf("expected localhost:8080, got %q", addr)
	}
}

func TestPublicURLListenAddrFromHostPort(t *testing.T) {
	if addr := publicURLListenAddr("localhost:8080"); addr != "localhost:8080" {
		t.Fatalf("expected localhost:8080, got %q", addr)
	}
}

func TestServerListenAddrPrefersExplicitListenAddr(t *testing.T) {
	addr := serverListenAddr(configWithPublicURL("http://localhost:8080", ":8080"))
	if addr != ":8080" {
		t.Fatalf("expected explicit listen address, got %q", addr)
	}
}

func TestServerListenAddrFallsBackToPublicURL(t *testing.T) {
	addr := serverListenAddr(configWithPublicURL("http://localhost:8080", ""))
	if addr != "localhost:8080" {
		t.Fatalf("expected public URL host fallback, got %q", addr)
	}
}

func configWithPublicURL(publicURL string, listenAddr string) config.RestConfig {
	return config.RestConfig{
		PublicURL:  publicURL,
		ListenAddr: listenAddr,
	}
}
