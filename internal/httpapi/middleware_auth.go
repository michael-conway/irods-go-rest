package httpapi

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/michael-conway/irods-go-rest/internal/auth"
)

const irodsTicketBearerPrefix = "irods-ticket:"

type ticketContextKey struct{}

type basicPasswordContextKey struct{}

func (h *Handler) requireBearer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authz, err := authorizationFromRequest(r)
		if err != nil {
			w.Header().Set("WWW-Authenticate", `Bearer realm="irods-go-rest", Basic realm="irods-go-rest"`)
			writeError(w, http.StatusUnauthorized, "missing_authorization", err.Error())
			return
		}

		switch authz.Scheme {
		case "basic":
			ctx := withPrincipal(r.Context(), auth.Principal{
				Subject:  authz.Username,
				Username: authz.Username,
				Scope:    []string{"basic"},
				Active:   true,
			})
			ctx = withBasicPassword(ctx, authz.Password)
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		case "bearer":
			principal, err := h.verifier.VerifyToken(r.Context(), authz.Token)
			if err != nil {
				status := http.StatusBadGateway
				errorCode := "auth_failed"
				if errors.Is(err, auth.ErrUnauthorized) {
					status = http.StatusUnauthorized
					errorCode = "invalid_token"
					w.Header().Set("WWW-Authenticate", `Bearer error="invalid_token"`)
				}
				if errors.Is(err, auth.ErrNotConfigured) {
					status = http.StatusInternalServerError
					errorCode = "auth_not_configured"
				}

				writeError(w, status, errorCode, err.Error())
				return
			}
			next.ServeHTTP(w, r.WithContext(withPrincipal(r.Context(), principal)))
			return
		default:
			w.Header().Set("WWW-Authenticate", `Bearer realm="irods-go-rest", Basic realm="irods-go-rest"`)
			writeError(w, http.StatusUnauthorized, "invalid_authorization", "unsupported authorization scheme")
			return
		}
	})
}

func withTicket(ctx context.Context, ticket string) context.Context {
	return context.WithValue(ctx, ticketContextKey{}, ticket)
}

func ticketFromContext(ctx context.Context) (string, bool) {
	ticket, ok := ctx.Value(ticketContextKey{}).(string)
	return ticket, ok
}

func withBasicPassword(ctx context.Context, password string) context.Context {
	return context.WithValue(ctx, basicPasswordContextKey{}, password)
}

func basicPasswordFromContext(ctx context.Context) (string, bool) {
	password, ok := ctx.Value(basicPasswordContextKey{}).(string)
	return password, ok
}

func (h *Handler) requireDownloadBearer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authz, err := authorizationFromRequest(r)
		if err != nil {
			w.Header().Set("WWW-Authenticate", `Bearer realm="irods-go-rest", Basic realm="irods-go-rest"`)
			writeError(w, http.StatusUnauthorized, "missing_authorization", err.Error())
			return
		}

		switch authz.Scheme {
		case "basic":
			ctx := withPrincipal(r.Context(), auth.Principal{
				Subject:  authz.Username,
				Username: authz.Username,
				Scope:    []string{"basic"},
				Active:   true,
			})
			ctx = withBasicPassword(ctx, authz.Password)
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		case "bearer-ticket":
			ticket := authz.Token
			next.ServeHTTP(w, r.WithContext(withTicket(r.Context(), ticket)))
			return
		case "bearer":
			principal, err := h.verifier.VerifyToken(r.Context(), authz.Token)
			if err != nil {
				status := http.StatusBadGateway
				errorCode := "auth_failed"
				if errors.Is(err, auth.ErrUnauthorized) {
					status = http.StatusUnauthorized
					errorCode = "invalid_token"
					w.Header().Set("WWW-Authenticate", `Bearer error="invalid_token"`)
				}
				if errors.Is(err, auth.ErrNotConfigured) {
					status = http.StatusInternalServerError
					errorCode = "auth_not_configured"
				}

				writeError(w, status, errorCode, err.Error())
				return
			}

			next.ServeHTTP(w, r.WithContext(withPrincipal(r.Context(), principal)))
			return
		default:
			w.Header().Set("WWW-Authenticate", `Bearer realm="irods-go-rest", Basic realm="irods-go-rest"`)
			writeError(w, http.StatusUnauthorized, "invalid_authorization", "unsupported authorization scheme")
			return
		}
	})
}

func parseIRODSTicketBearer(token string) (string, bool) {
	token = strings.TrimSpace(token)
	if !strings.HasPrefix(strings.ToLower(token), irodsTicketBearerPrefix) {
		return "", false
	}

	ticket := strings.TrimSpace(token[len(irodsTicketBearerPrefix):])
	if ticket == "" {
		return "", false
	}

	return ticket, true
}

type requestAuthorization struct {
	Scheme   string
	Token    string
	Username string
	Password string
}

func authorizationFromRequest(r *http.Request) (requestAuthorization, error) {
	header := strings.TrimSpace(r.Header.Get("Authorization"))
	if header == "" {
		return requestAuthorization{}, fmt.Errorf("authorization header must contain a bearer token or basic credentials")
	}

	scheme, value, found := strings.Cut(header, " ")
	if !found || strings.TrimSpace(value) == "" {
		return requestAuthorization{}, fmt.Errorf("expected Authorization: Bearer <token> or Basic <credentials>")
	}

	value = strings.TrimSpace(value)
	switch {
	case strings.EqualFold(scheme, "Bearer"):
		if ticket, ok := parseIRODSTicketBearer(value); ok {
			return requestAuthorization{Scheme: "bearer-ticket", Token: ticket}, nil
		}
		return requestAuthorization{Scheme: "bearer", Token: value}, nil
	case strings.EqualFold(scheme, "Basic"):
		username, password, err := basicCredentialsFromHeader(value)
		if err != nil {
			return requestAuthorization{}, err
		}
		return requestAuthorization{Scheme: "basic", Username: username, Password: password}, nil
	default:
		return requestAuthorization{}, fmt.Errorf("expected Authorization: Bearer <token> or Basic <credentials>")
	}
}

func basicCredentialsFromHeader(value string) (string, string, error) {
	decoded, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return "", "", fmt.Errorf("invalid Basic authorization value")
	}

	credentials := string(decoded)
	username, password, found := strings.Cut(credentials, ":")
	if !found || strings.TrimSpace(username) == "" {
		return "", "", fmt.Errorf("invalid Basic authorization value")
	}

	return strings.TrimSpace(username), strings.TrimSpace(password), nil
}
