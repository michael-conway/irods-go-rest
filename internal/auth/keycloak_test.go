package auth

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/michael-conway/irods-go-rest/internal/config"
)

func TestAuthorizationURL(t *testing.T) {
	service := NewKeycloakService(config.Config{
		KeycloakURL:    "http://keycloak.local",
		KeycloakRealm:  "demo",
		KeycloakClient: "irods-rest",
		PublicURL:      "http://localhost:8080",
		AuthScopes:     "openid profile email",
	})

	redirectURL, err := service.AuthorizationURL("state123")
	if err != nil {
		t.Fatalf("authorization url failed: %v", err)
	}

	parsed, err := url.Parse(redirectURL)
	if err != nil {
		t.Fatalf("parse authorization url: %v", err)
	}

	if parsed.Path != "/realms/demo/protocol/openid-connect/auth" {
		t.Fatalf("unexpected path: %s", parsed.Path)
	}

	assertQueryValue(t, parsed.Query(), "client_id", "irods-rest")
	assertQueryValue(t, parsed.Query(), "redirect_uri", "http://localhost:8080/web/callback")
	assertQueryValue(t, parsed.Query(), "response_type", "code")
	assertQueryValue(t, parsed.Query(), "scope", "openid profile email")
	assertQueryValue(t, parsed.Query(), "state", "state123")
}

func TestExchangeCodeSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/realms/demo/protocol/openid-connect/token" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}

		if r.Method != http.MethodPost {
			t.Fatalf("unexpected method: %s", r.Method)
		}

		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}

		assertQueryValue(t, r.Form, "grant_type", "authorization_code")
		assertQueryValue(t, r.Form, "client_id", "irods-rest")
		assertQueryValue(t, r.Form, "client_secret", "secret")
		assertQueryValue(t, r.Form, "code", "code123")
		assertQueryValue(t, r.Form, "redirect_uri", "http://localhost:8080/web/callback")

		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  "abc123",
			"token_type":    "Bearer",
			"expires_in":    300,
			"refresh_token": "refresh123",
			"id_token":      "id123",
			"scope":         "openid profile email",
		})
	}))
	defer server.Close()

	service := NewKeycloakService(config.Config{
		KeycloakURL:    server.URL,
		KeycloakRealm:  "demo",
		KeycloakClient: "irods-rest",
		KeycloakSecret: "secret",
		PublicURL:      "http://localhost:8080",
	})

	token, err := service.ExchangeCode(context.Background(), "code123")
	if err != nil {
		t.Fatalf("exchange failed: %v", err)
	}

	if token.AccessToken != "abc123" {
		t.Fatalf("expected access token, got %q", token.AccessToken)
	}
}

func TestVerifyTokenSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/realms/demo/protocol/openid-connect/token/introspect" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}

		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}

		assertQueryValue(t, r.Form, "token", "abc123")
		assertQueryValue(t, r.Form, "client_id", "irods-rest")
		assertQueryValue(t, r.Form, "client_secret", "secret")

		_ = json.NewEncoder(w).Encode(map[string]any{
			"active":             true,
			"scope":              "openid profile",
			"preferred_username": "alice",
			"sub":                "user-123",
		})
	}))
	defer server.Close()

	service := NewKeycloakService(config.Config{
		KeycloakURL:    server.URL,
		KeycloakRealm:  "demo",
		KeycloakClient: "irods-rest",
		KeycloakSecret: "secret",
		PublicURL:      "http://localhost:8080",
	})

	principal, err := service.VerifyToken(context.Background(), "abc123")
	if err != nil {
		t.Fatalf("verify token failed: %v", err)
	}

	if principal.Username != "alice" {
		t.Fatalf("expected principal username alice, got %q", principal.Username)
	}
}

func TestNewState(t *testing.T) {
	service := NewKeycloakService(config.Config{})

	state, err := service.NewState()
	if err != nil {
		t.Fatalf("new state failed: %v", err)
	}

	if strings.TrimSpace(state) == "" {
		t.Fatal("expected non-empty state")
	}
}

func assertQueryValue(t *testing.T, values url.Values, key string, expected string) {
	t.Helper()

	if got := values.Get(key); got != expected {
		t.Fatalf("expected %s=%q, got %q", key, expected, got)
	}
}
