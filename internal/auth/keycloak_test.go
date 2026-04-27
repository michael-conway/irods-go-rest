package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/michael-conway/irods-go-rest/internal/config"
)

func TestAuthorizationURL(t *testing.T) {
	cfg := keycloakUnitTestConfig(t)
	service := NewKeycloakService(cfg)

	redirectURL, err := service.AuthorizationURL("state123")
	if err != nil {
		t.Fatalf("authorization url failed: %v", err)
	}

	parsed, err := url.Parse(redirectURL)
	if err != nil {
		t.Fatalf("parse authorization url: %v", err)
	}

	if parsed.Path != fmt.Sprintf("/realms/%s/protocol/openid-connect/auth", cfg.OidcRealm) {
		t.Fatalf("unexpected path: %s", parsed.Path)
	}

	assertQueryValue(t, parsed.Query(), "client_id", cfg.OidcClientId)
	assertQueryValue(t, parsed.Query(), "redirect_uri", strings.TrimRight(cfg.PublicURL, "/")+"/web/callback")
	assertQueryValue(t, parsed.Query(), "response_type", "code")
	assertQueryValue(t, parsed.Query(), "scope", cfg.OidcScope)
	assertQueryValue(t, parsed.Query(), "state", "state123")
}

func TestExchangeCodeSuccess(t *testing.T) {
	cfg := keycloakUnitTestConfig(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != fmt.Sprintf("/realms/%s/protocol/openid-connect/token", cfg.OidcRealm) {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}

		if r.Method != http.MethodPost {
			t.Fatalf("unexpected method: %s", r.Method)
		}

		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}

		assertQueryValue(t, r.Form, "grant_type", "authorization_code")
		assertQueryValue(t, r.Form, "client_id", cfg.OidcClientId)
		assertQueryValue(t, r.Form, "client_secret", cfg.OidcClientSecret)
		assertQueryValue(t, r.Form, "code", "code123")
		assertQueryValue(t, r.Form, "redirect_uri", strings.TrimRight(cfg.PublicURL, "/")+"/web/callback")

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

	cfg.OidcUrl = server.URL
	service := NewKeycloakService(cfg)

	token, err := service.ExchangeCode(context.Background(), "code123")
	if err != nil {
		t.Fatalf("exchange failed: %v", err)
	}

	if token.AccessToken != "abc123" {
		t.Fatalf("expected access token, got %q", token.AccessToken)
	}
}

func TestVerifyTokenSuccess(t *testing.T) {
	cfg := keycloakUnitTestConfig(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != fmt.Sprintf("/realms/%s/protocol/openid-connect/token/introspect", cfg.OidcRealm) {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}

		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}

		assertQueryValue(t, r.Form, "token", "abc123")
		assertQueryValue(t, r.Form, "client_id", cfg.OidcClientId)
		assertQueryValue(t, r.Form, "client_secret", cfg.OidcClientSecret)

		_ = json.NewEncoder(w).Encode(map[string]any{
			"active":             true,
			"scope":              "openid profile",
			"preferred_username": "alice",
			"sub":                "user-123",
		})
	}))
	defer server.Close()

	cfg.OidcUrl = server.URL
	service := NewKeycloakService(cfg)

	principal, err := service.VerifyToken(context.Background(), "abc123")
	if err != nil {
		t.Fatalf("verify token failed: %v", err)
	}

	if principal.Username != "alice" {
		t.Fatalf("expected principal username alice, got %q", principal.Username)
	}
}

func TestNewState(t *testing.T) {
	service := NewKeycloakService(keycloakUnitTestConfig(t))

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

func keycloakUnitTestConfig(t *testing.T) config.RestConfig {
	t.Helper()

	repoRoot, err := authRepoRoot()
	if err != nil {
		t.Fatalf("resolve repo root: %v", err)
	}

	cfg, err := keycloakUnitTestRestConfig(repoRoot)
	if err != nil {
		t.Fatalf("load unit test rest config: %v", err)
	}

	if strings.TrimSpace(cfg.PublicURL) == "" {
		cfg.PublicURL = "http://localhost:8080"
	}
	if strings.TrimSpace(cfg.OidcScope) == "" {
		cfg.OidcScope = "openid profile email"
	}

	requireConfiguredTestValue(t, "OidcUrl", cfg.OidcUrl)
	requireConfiguredTestValue(t, "OidcRealm", cfg.OidcRealm)
	requireConfiguredTestValue(t, "OidcClientId", cfg.OidcClientId)
	requireConfiguredTestValue(t, "OidcClientSecret", cfg.OidcClientSecret)

	return cfg
}

func keycloakUnitTestRestConfig(repoRoot string) (config.RestConfig, error) {
	configFile := strings.TrimSpace(os.Getenv("GOREST_E2E_CONFIG_FILE"))
	if configFile == "" {
		return config.RestConfig{}, nil
	}

	resolvedConfigPath, err := resolveAuthConfigPath(repoRoot, configFile)
	if err != nil {
		return config.RestConfig{}, err
	}

	originalConfigFile := os.Getenv(config.ConfigFileEnvVar)
	if err := os.Setenv(config.ConfigFileEnvVar, resolvedConfigPath); err != nil {
		return config.RestConfig{}, fmt.Errorf("set %s: %w", config.ConfigFileEnvVar, err)
	}
	defer func() {
		_ = os.Setenv(config.ConfigFileEnvVar, originalConfigFile)
	}()

	cfg, err := config.ReadRestConfig("", "", nil)
	if err != nil {
		return config.RestConfig{}, fmt.Errorf("read rest config from GOREST_E2E_CONFIG_FILE=%q: %w", resolvedConfigPath, err)
	}

	return *cfg, nil
}

func resolveAuthConfigPath(repoRoot string, configFile string) (string, error) {
	configFile = strings.TrimSpace(configFile)
	if configFile == "" {
		return "", fmt.Errorf("empty config file path")
	}

	if filepath.IsAbs(configFile) {
		return configFile, nil
	}

	return filepath.Join(repoRoot, configFile), nil
}

func authRepoRoot() (string, error) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("runtime caller unavailable")
	}

	internalDir := filepath.Dir(filepath.Dir(filename))
	return filepath.Dir(internalDir), nil
}

func requireConfiguredTestValue(t *testing.T, field string, value string) {
	t.Helper()

	if strings.TrimSpace(value) == "" {
		t.Skipf("keycloak unit tests require %s in GOREST_E2E_CONFIG_FILE", field)
	}
}
