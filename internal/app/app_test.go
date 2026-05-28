package app

import (
	"testing"
	"time"

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

func TestNewAppliesHTTPServerTimeoutSettings(t *testing.T) {
	cfg := configWithPublicURL("http://localhost:8080", "")
	cfg.HTTPReadTimeoutSeconds = 31
	cfg.HTTPReadHeaderTimeoutSeconds = 6
	cfg.HTTPWriteTimeoutSeconds = 32
	cfg.HTTPIdleTimeoutSeconds = 121
	cfg.HTTPMaxHeaderBytes = 123456

	app := New(cfg)

	if app.server.ReadTimeout != 31*time.Second {
		t.Fatalf("expected ReadTimeout 31s, got %s", app.server.ReadTimeout)
	}
	if app.server.ReadHeaderTimeout != 6*time.Second {
		t.Fatalf("expected ReadHeaderTimeout 6s, got %s", app.server.ReadHeaderTimeout)
	}
	if app.server.WriteTimeout != 32*time.Second {
		t.Fatalf("expected WriteTimeout 32s, got %s", app.server.WriteTimeout)
	}
	if app.server.IdleTimeout != 121*time.Second {
		t.Fatalf("expected IdleTimeout 121s, got %s", app.server.IdleTimeout)
	}
	if app.server.MaxHeaderBytes != 123456 {
		t.Fatalf("expected MaxHeaderBytes 123456, got %d", app.server.MaxHeaderBytes)
	}
}

func configWithPublicURL(publicURL string, listenAddr string) config.RestConfig {
	return config.RestConfig{
		PublicURL:  publicURL,
		ListenAddr: listenAddr,
	}
}
