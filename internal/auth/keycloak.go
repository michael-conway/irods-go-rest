package auth

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/michael-conway/irods-go-rest/internal/config"
)

var (
	ErrNotConfigured   = errors.New("keycloak auth is not configured")
	ErrInvalidCallback = errors.New("invalid auth callback")
	ErrUnauthorized    = errors.New("unauthorized")
)

type Token struct {
	AccessToken  string `json:"accessToken"`
	TokenType    string `json:"tokenType"`
	ExpiresIn    int    `json:"expiresIn"`
	RefreshToken string `json:"refreshToken,omitempty"`
	IDToken      string `json:"idToken,omitempty"`
	Scope        string `json:"scope,omitempty"`
}

type Principal struct {
	Subject  string   `json:"subject,omitempty"`
	Username string   `json:"username,omitempty"`
	Scope    []string `json:"scope,omitempty"`
	Active   bool     `json:"active"`
}

type AuthFlowService interface {
	AuthorizationURL(state string) (string, error)
	ExchangeCode(ctx context.Context, code string) (Token, error)
	NewState() (string, error)
}

type TokenVerifier interface {
	VerifyToken(ctx context.Context, accessToken string) (Principal, error)
}

type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type KeycloakService struct {
	httpClient   HTTPClient
	baseURL      string
	realm        string
	clientID     string
	clientSecret string
	redirectURL  string
	scopes       string
}

func NewKeycloakService(cfg config.Config) *KeycloakService {
	return &KeycloakService{
		httpClient:   &http.Client{Timeout: 10 * time.Second},
		baseURL:      strings.TrimRight(cfg.KeycloakURL, "/"),
		realm:        cfg.KeycloakRealm,
		clientID:     cfg.KeycloakClient,
		clientSecret: cfg.KeycloakSecret,
		redirectURL:  strings.TrimRight(cfg.PublicURL, "/") + "/web/callback",
		scopes:       cfg.AuthScopes,
	}
}

func (k *KeycloakService) AuthorizationURL(state string) (string, error) {
	if k == nil || k.baseURL == "" || k.realm == "" || k.clientID == "" || k.redirectURL == "" {
		return "", ErrNotConfigured
	}

	if strings.TrimSpace(state) == "" {
		return "", fmt.Errorf("%w: missing state", ErrInvalidCallback)
	}

	authURL, err := url.Parse(fmt.Sprintf("%s/realms/%s/protocol/openid-connect/auth", k.baseURL, url.PathEscape(k.realm)))
	if err != nil {
		return "", fmt.Errorf("build authorization url: %w", err)
	}

	query := authURL.Query()
	query.Set("client_id", k.clientID)
	query.Set("redirect_uri", k.redirectURL)
	query.Set("response_type", "code")
	query.Set("scope", k.scopes)
	query.Set("state", state)
	authURL.RawQuery = query.Encode()

	return authURL.String(), nil
}

func (k *KeycloakService) ExchangeCode(ctx context.Context, code string) (Token, error) {
	if k == nil || k.baseURL == "" || k.realm == "" || k.clientID == "" || k.redirectURL == "" {
		return Token{}, ErrNotConfigured
	}

	if strings.TrimSpace(code) == "" {
		return Token{}, fmt.Errorf("%w: missing code", ErrInvalidCallback)
	}

	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("client_id", k.clientID)
	form.Set("code", code)
	form.Set("redirect_uri", k.redirectURL)
	if k.clientSecret != "" {
		form.Set("client_secret", k.clientSecret)
	}

	tokenURL := fmt.Sprintf("%s/realms/%s/protocol/openid-connect/token", k.baseURL, url.PathEscape(k.realm))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return Token{}, fmt.Errorf("build keycloak token request: %w", err)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := k.httpClient.Do(req)
	if err != nil {
		return Token{}, fmt.Errorf("request keycloak token: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return Token{}, fmt.Errorf("keycloak token request failed: %s", resp.Status)
	}

	var payload struct {
		AccessToken  string `json:"access_token"`
		TokenType    string `json:"token_type"`
		ExpiresIn    int    `json:"expires_in"`
		RefreshToken string `json:"refresh_token"`
		IDToken      string `json:"id_token"`
		Scope        string `json:"scope"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return Token{}, fmt.Errorf("decode keycloak token response: %w", err)
	}

	if payload.AccessToken == "" {
		return Token{}, fmt.Errorf("keycloak token response missing access_token")
	}

	return Token{
		AccessToken:  payload.AccessToken,
		TokenType:    payload.TokenType,
		ExpiresIn:    payload.ExpiresIn,
		RefreshToken: payload.RefreshToken,
		IDToken:      payload.IDToken,
		Scope:        payload.Scope,
	}, nil
}

func (k *KeycloakService) NewState() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate oauth state: %w", err)
	}

	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func (k *KeycloakService) VerifyToken(ctx context.Context, accessToken string) (Principal, error) {
	if k == nil || k.baseURL == "" || k.realm == "" || k.clientID == "" {
		return Principal{}, ErrNotConfigured
	}

	if strings.TrimSpace(accessToken) == "" {
		return Principal{}, ErrUnauthorized
	}

	form := url.Values{}
	form.Set("token", accessToken)
	form.Set("client_id", k.clientID)
	if k.clientSecret != "" {
		form.Set("client_secret", k.clientSecret)
	}

	introspectURL := fmt.Sprintf("%s/realms/%s/protocol/openid-connect/token/introspect", k.baseURL, url.PathEscape(k.realm))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, introspectURL, strings.NewReader(form.Encode()))
	if err != nil {
		return Principal{}, fmt.Errorf("build keycloak introspection request: %w", err)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := k.httpClient.Do(req)
	if err != nil {
		return Principal{}, fmt.Errorf("request keycloak introspection: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return Principal{}, fmt.Errorf("keycloak introspection failed: %s", resp.Status)
	}

	var payload struct {
		Active            bool   `json:"active"`
		Scope             string `json:"scope"`
		PreferredUsername string `json:"preferred_username"`
		Sub               string `json:"sub"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return Principal{}, fmt.Errorf("decode keycloak introspection response: %w", err)
	}

	if !payload.Active {
		return Principal{}, ErrUnauthorized
	}

	return Principal{
		Subject:  payload.Sub,
		Username: payload.PreferredUsername,
		Scope:    strings.Fields(payload.Scope),
		Active:   payload.Active,
	}, nil
}
