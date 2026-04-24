package httpapi

import (
	"context"
	"net/http"
	"strings"

	api "github.com/michael-conway/irods-go-rest/api"
	"github.com/michael-conway/irods-go-rest/internal/auth"
	"github.com/michael-conway/irods-go-rest/internal/config"
	"github.com/michael-conway/irods-go-rest/internal/irods"
)

const swaggerUIHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>iRODS REST API Docs</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>
    window.ui = SwaggerUIBundle({
      url: "/openapi.yaml",
      dom_id: "#swagger-ui"
    });
  </script>
</body>
</html>
`

type Handler struct {
	cfg        config.RestConfig
	catalog    irods.CatalogService
	authFlow   auth.AuthFlowService
	verifier   auth.TokenVerifier
	webSession *auth.SessionStore
}

func NewHandler(cfg config.RestConfig, catalog irods.CatalogService, authFlow auth.AuthFlowService, verifier auth.TokenVerifier, webSession *auth.SessionStore) *Handler {
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
	mux.HandleFunc("GET /openapi.yaml", h.getOpenAPISpec)
	mux.HandleFunc("GET /swagger", h.getSwaggerUI)
	mux.HandleFunc("GET /web/", h.webHome)
	mux.HandleFunc("GET /web/login", h.webLogin)
	mux.HandleFunc("GET /web/callback", h.webCallback)
	mux.HandleFunc("POST /web/logout", h.webLogout)
	mux.Handle("GET /api/v1/objects/{object_id}", h.requireBearer(http.HandlerFunc(h.getObject)))
	mux.Handle("GET /api/v1/collections/{collection_id}", h.requireBearer(http.HandlerFunc(h.getCollection)))
	mux.Handle("GET /api/v1/data-objects/by-path", h.requireBearer(http.HandlerFunc(h.getObjectByPath)))
	mux.Handle("HEAD /api/v1/data-objects/content", h.requireDownloadBearer(http.HandlerFunc(h.headObjectContentByPath)))
	mux.Handle("GET /api/v1/data-objects/content", h.requireDownloadBearer(http.HandlerFunc(h.getObjectContentByPath)))

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

func (h *Handler) getOpenAPISpec(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/yaml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(api.OpenAPISpec)
}

func (h *Handler) getSwaggerUI(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(swaggerUIHTML))
}
