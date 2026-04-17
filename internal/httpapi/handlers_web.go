package httpapi

import (
	"errors"
	"fmt"
	"html"
	"net/http"

	"github.com/michael-conway/irods-go-rest/internal/auth"
)

const (
	authStateCookieName  = "irods_rest_oauth_state"
	webSessionCookieName = "irods_rest_web_session"
)

func (h *Handler) webHome(w http.ResponseWriter, r *http.Request) {
	session, ok := h.currentWebSession(r)
	if !ok {
		writeHTML(w, http.StatusOK, `<html><body><h1>iRODS REST Web Login</h1><p>No active web session.</p><p><a href="/web/login">Sign in with Keycloak</a></p></body></html>`)
		return
	}

	username := html.EscapeString(session.Principal.Username)
	if username == "" {
		username = html.EscapeString(session.Principal.Subject)
	}

	body := fmt.Sprintf(`<html><body><h1>iRODS REST Web Login</h1><p>Signed in as <strong>%s</strong>.</p><p>This browser session is separate from the bearer-token API.</p><form method="post" action="/web/logout"><button type="submit">Sign out</button></form></body></html>`, username)
	writeHTML(w, http.StatusOK, body)
}

func (h *Handler) webLogin(w http.ResponseWriter, r *http.Request) {
	state, err := h.authFlow.NewState()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "auth_failed", err.Error())
		return
	}

	redirectURL, err := h.authFlow.AuthorizationURL(state)
	if err != nil {
		status := http.StatusBadGateway
		code := "auth_failed"
		if errors.Is(err, auth.ErrNotConfigured) {
			status = http.StatusInternalServerError
			code = "auth_not_configured"
		}

		writeError(w, status, code, err.Error())
		return
	}

	http.SetCookie(w, &http.Cookie{
		Name:     authStateCookieName,
		Value:    state,
		Path:     "/web",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   r.TLS != nil,
	})

	http.Redirect(w, r, redirectURL, http.StatusFound)
}

func (h *Handler) webCallback(w http.ResponseWriter, r *http.Request) {
	code := r.URL.Query().Get("code")
	state := r.URL.Query().Get("state")
	if code == "" || state == "" {
		writeError(w, http.StatusBadRequest, "invalid_callback", "callback must include code and state")
		return
	}

	stateCookie, err := r.Cookie(authStateCookieName)
	if err != nil || stateCookie.Value == "" || stateCookie.Value != state {
		writeError(w, http.StatusBadRequest, "invalid_callback", "oauth state validation failed")
		return
	}

	token, err := h.authFlow.ExchangeCode(r.Context(), code)
	if err != nil {
		status := http.StatusBadGateway
		responseCode := "auth_failed"
		if errors.Is(err, auth.ErrNotConfigured) {
			status = http.StatusInternalServerError
			responseCode = "auth_not_configured"
		}
		if errors.Is(err, auth.ErrInvalidCallback) {
			status = http.StatusBadRequest
			responseCode = "invalid_callback"
		}

		writeError(w, status, responseCode, err.Error())
		return
	}

	principal, err := h.verifier.VerifyToken(r.Context(), token.AccessToken)
	if err != nil {
		writeError(w, http.StatusBadGateway, "auth_failed", err.Error())
		return
	}

	session, err := h.webSession.Create(principal, token)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "auth_failed", err.Error())
		return
	}

	clearWebCookie(w, authStateCookieName, r)
	http.SetCookie(w, &http.Cookie{
		Name:     webSessionCookieName,
		Value:    session.ID,
		Path:     "/web",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   r.TLS != nil,
	})

	http.Redirect(w, r, "/web/", http.StatusFound)
}

func (h *Handler) webLogout(w http.ResponseWriter, r *http.Request) {
	if cookie, err := r.Cookie(webSessionCookieName); err == nil && cookie.Value != "" {
		h.webSession.Delete(cookie.Value)
	}

	clearWebCookie(w, webSessionCookieName, r)
	http.Redirect(w, r, "/web/", http.StatusFound)
}

func clearWebCookie(w http.ResponseWriter, name string, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name:     name,
		Value:    "",
		Path:     "/web",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   r.TLS != nil,
		MaxAge:   -1,
	})
}

func (h *Handler) currentWebSession(r *http.Request) (auth.Session, bool) {
	cookie, err := r.Cookie(webSessionCookieName)
	if err != nil || cookie.Value == "" {
		return auth.Session{}, false
	}

	return h.webSession.Get(cookie.Value)
}
