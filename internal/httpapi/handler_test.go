package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/michael-conway/irods-go-rest/internal/auth"
	"github.com/michael-conway/irods-go-rest/internal/config"
	"github.com/michael-conway/irods-go-rest/internal/irods"
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

	req := httptest.NewRequest(http.MethodGet, "/api/v1/objects/demo-object", nil)
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestAPIAcceptsValidBearerToken(t *testing.T) {
	handler := testHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/objects/demo-object", nil)
	req.Header.Set("Authorization", "Bearer token123")
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
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
	return NewHandler(*cfg, irods.NewCatalogService(*cfg), stubAuthService{}, stubAuthService{}, auth.NewSessionStore())
}

func containsAll(s string, parts ...string) bool {
	for _, part := range parts {
		if !strings.Contains(s, part) {
			return false
		}
	}
	return true
}
