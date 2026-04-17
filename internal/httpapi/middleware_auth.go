package httpapi

import (
	"errors"
	"net/http"
	"strings"

	"github.com/michael-conway/irods-go-rest/internal/auth"
)

func (h *Handler) requireBearer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := strings.TrimSpace(r.Header.Get("Authorization"))
		if !strings.HasPrefix(strings.ToLower(header), "bearer ") {
			w.Header().Set("WWW-Authenticate", `Bearer realm="irods-go-rest"`)
			writeError(w, http.StatusUnauthorized, "missing_bearer_token", "authorization header must contain a bearer token")
			return
		}

		token := strings.TrimSpace(header[len("Bearer "):])
		principal, err := h.verifier.VerifyToken(r.Context(), token)
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
	})
}
