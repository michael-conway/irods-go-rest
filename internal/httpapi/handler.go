package httpapi

import (
	"context"
	"net/http"
	"strings"

	"github.com/michael-conway/irods-go-rest/internal/auth"
	"github.com/michael-conway/irods-go-rest/internal/config"
	"github.com/michael-conway/irods-go-rest/internal/irods"
)

type Handler struct {
	cfg        config.Config
	catalog    irods.CatalogService
	authFlow   auth.AuthFlowService
	verifier   auth.TokenVerifier
	webSession *auth.SessionStore
}

func NewHandler(cfg config.Config, catalog irods.CatalogService, authFlow auth.AuthFlowService, verifier auth.TokenVerifier, webSession *auth.SessionStore) *Handler {
	return &Handler{
		cfg:        cfg,
		catalog:    catalog,
		authFlow:   authFlow,
		verifier:   verifier,
		webSession: webSession,
	}
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", h.getHealth)
	mux.HandleFunc("GET /web/", h.webHome)
	mux.HandleFunc("GET /web/login", h.webLogin)
	mux.HandleFunc("GET /web/callback", h.webCallback)
	mux.HandleFunc("POST /web/logout", h.webLogout)
	mux.Handle("GET /api/v1/objects/{object_id}", h.requireBearer(http.HandlerFunc(h.getObject)))
	mux.Handle("GET /api/v1/collections/{collection_id}", h.requireBearer(http.HandlerFunc(h.getCollection)))

	return requestLogger(mux)
}

func pathValue(r *http.Request, key string) string {
	return strings.TrimSpace(r.PathValue(key))
}

type principalContextKey struct{}

func withPrincipal(ctx context.Context, principal auth.Principal) context.Context {
	return context.WithValue(ctx, principalContextKey{}, principal)
}

func principalFromContext(ctx context.Context) (auth.Principal, bool) {
	principal, ok := ctx.Value(principalContextKey{}).(auth.Principal)
	return principal, ok
}
