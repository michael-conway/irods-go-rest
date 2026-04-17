package httpapi

import (
	"net/http"
	"strings"

	"github.com/michael-conway/irods-go-rest/internal/config"
	"github.com/michael-conway/irods-go-rest/internal/irods"
)

type Handler struct {
	cfg     config.Config
	catalog irods.CatalogService
}

func NewHandler(cfg config.Config, catalog irods.CatalogService) *Handler {
	return &Handler{
		cfg:     cfg,
		catalog: catalog,
	}
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", h.getHealth)
	mux.HandleFunc("GET /api/v1/objects/{object_id}", h.getObject)
	mux.HandleFunc("GET /api/v1/collections/{collection_id}", h.getCollection)

	return requestLogger(mux)
}

func pathValue(r *http.Request, key string) string {
	return strings.TrimSpace(r.PathValue(key))
}
