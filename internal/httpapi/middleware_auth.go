package httpapi

import (
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/michael-conway/go-irodsclient-extensions/tickets"
	"github.com/michael-conway/irods-go-rest/internal/auth"
	"github.com/michael-conway/irods-go-rest/internal/logutil"
)

func (h *Handler) requireBearer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authz, err := authorizationFromRequest(r)
		if err != nil {
			logAuthMiddlewareError("authorization header parse failed", err, r, "phase", "requireBearer")
			w.Header().Set("WWW-Authenticate", `Bearer realm="irods-go-rest", Basic realm="irods-go-rest"`)
			writeError(w, http.StatusUnauthorized, "missing_authorization", err.Error())
			return
		}

		switch authz.Scheme {
		case "basic":
			slog.Debug("http auth resolved basic credentials", "path", r.URL.Path, "username", authz.Username)
			ctx := auth.WithPrincipal(r.Context(), auth.Principal{
				Subject:  authz.Username,
				Username: authz.Username,
				Scope:    []string{"basic"},
				Active:   true,
			})
			ctx = auth.WithBasicPassword(ctx, authz.Password)
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		case "bearer":
			principal, err := h.verifier.VerifyToken(r.Context(), authz.Token)
			if err != nil {
				logAuthMiddlewareError("bearer token verification failed", err, r, "phase", "requireBearer", "auth_scheme", authz.Scheme)
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
			next.ServeHTTP(w, r.WithContext(auth.WithPrincipal(r.Context(), principal)))
			return
		default:
			w.Header().Set("WWW-Authenticate", `Bearer realm="irods-go-rest", Basic realm="irods-go-rest"`)
			writeError(w, http.StatusUnauthorized, "invalid_authorization", "unsupported authorization scheme")
			return
		}
	})
}

func (h *Handler) requireDownloadBearer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ticketID := queryTicketID(r); ticketID != "" {
			slog.Debug("http download auth resolved iRODS ticket from query parameter", "path", r.URL.Path)
			next.ServeHTTP(w, r.WithContext(auth.WithTicket(r.Context(), ticketID)))
			return
		}

		authz, err := authorizationFromRequest(r)
		if err != nil {
			logAuthMiddlewareError("authorization header parse failed", err, r, "phase", "requireDownloadBearer")
			w.Header().Set("WWW-Authenticate", `Bearer realm="irods-go-rest", Basic realm="irods-go-rest"`)
			writeError(w, http.StatusUnauthorized, "missing_authorization", err.Error())
			return
		}

		switch authz.Scheme {
		case "basic":
			slog.Debug("http download auth resolved basic credentials", "path", r.URL.Path, "username", authz.Username)
			ctx := auth.WithPrincipal(r.Context(), auth.Principal{
				Subject:  authz.Username,
				Username: authz.Username,
				Scope:    []string{"basic"},
				Active:   true,
			})
			ctx = auth.WithBasicPassword(ctx, authz.Password)
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		case "bearer-ticket":
			slog.Debug("http download auth resolved iRODS ticket", "path", r.URL.Path)
			ticket := authz.Token
			next.ServeHTTP(w, r.WithContext(auth.WithTicket(r.Context(), ticket)))
			return
		case "bearer":
			principal, err := h.verifier.VerifyToken(r.Context(), authz.Token)
			if err != nil {
				logAuthMiddlewareError("download bearer token verification failed", err, r, "phase", "requireDownloadBearer", "auth_scheme", authz.Scheme)
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

			next.ServeHTTP(w, r.WithContext(auth.WithPrincipal(r.Context(), principal)))
			return
		default:
			w.Header().Set("WWW-Authenticate", `Bearer realm="irods-go-rest", Basic realm="irods-go-rest"`)
			writeError(w, http.StatusUnauthorized, "invalid_authorization", "unsupported authorization scheme")
			return
		}
	})
}

func parseIRODSTicketBearer(token string) (string, bool) {
	return tickets.ParseBearerToken(token)
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

func logAuthMiddlewareError(msg string, err error, r *http.Request, args ...any) {
	logArgs := []any{
		"error", err.Error(),
		"stack_trace", logutil.StackTrace(),
		"method", r.Method,
		"path", r.URL.Path,
	}
	logArgs = append(logArgs, args...)
	slog.Error(msg, logArgs...)
}
