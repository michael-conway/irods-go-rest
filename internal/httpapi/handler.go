package httpapi

import (
	"net/http"
	"strings"

	api "github.com/michael-conway/irods-go-rest/api"
	"github.com/michael-conway/irods-go-rest/internal/auth"
	"github.com/michael-conway/irods-go-rest/internal/config"
	"github.com/michael-conway/irods-go-rest/internal/restservice"
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
	paths      restservice.PathService
	authFlow   auth.AuthFlowService
	verifier   auth.TokenVerifier
	webSession *auth.SessionStore
}

func NewHandler(cfg config.RestConfig, paths restservice.PathService, authFlow auth.AuthFlowService, verifier auth.TokenVerifier, webSession *auth.SessionStore) *Handler {
	return &Handler{
		cfg:        cfg,
		paths:      paths,
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
	mux.Handle("GET /api/v1/path", h.requireBearer(http.HandlerFunc(h.getPath)))
	mux.Handle("GET /api/v1/path/children", h.requireBearer(http.HandlerFunc(h.getPathChildren)))
	mux.Handle("GET /api/v1/path/avu", h.requireBearer(http.HandlerFunc(h.getPathAVUs)))
	mux.Handle("POST /api/v1/path/avu", h.requireBearer(http.HandlerFunc(h.postPathAVU)))
	mux.Handle("PUT /api/v1/path/avu/{avu_id}", h.requireBearer(http.HandlerFunc(h.putPathAVU)))
	mux.Handle("DELETE /api/v1/path/avu/{avu_id}", h.requireBearer(http.HandlerFunc(h.deletePathAVU)))
	mux.Handle("GET /api/v1/path/checksum", h.requireBearer(http.HandlerFunc(h.getPathChecksum)))
	mux.Handle("POST /api/v1/path/checksum", h.requireBearer(http.HandlerFunc(h.postPathChecksum)))
	mux.Handle("HEAD /api/v1/path/contents", h.requireDownloadBearer(http.HandlerFunc(h.headPathContents)))
	mux.Handle("GET /api/v1/path/contents", h.requireDownloadBearer(http.HandlerFunc(h.getPathContents)))

	return requestLogger(mux)
}

func pathValue(r *http.Request, key string) string {
	return strings.TrimSpace(r.PathValue(key))
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
