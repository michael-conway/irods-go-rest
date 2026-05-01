package httpapi

import (
	"errors"
	"net/http"
	"strings"

	"github.com/michael-conway/irods-go-rest/internal/domain"
	"github.com/michael-conway/irods-go-rest/internal/irods"
)

func (h *Handler) getResources(w http.ResponseWriter, r *http.Request) {
	scope := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("scope")))
	if scope == "" {
		scope = "top"
	}
	if scope != "top" && scope != "all" {
		writeError(w, http.StatusBadRequest, "invalid_request", "scope must be top or all")
		return
	}

	resources, err := h.resources.ListResources(r.Context(), scope)
	if err != nil {
		writeResourceError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"resources": resources,
		"count":     len(resources),
		"scope":     scope,
		"links": map[string]any{
			"self": domain.ActionLink{
				Href:   "/api/v1/resource?scope=" + scope,
				Method: http.MethodGet,
			},
		},
	})
}

func (h *Handler) getResource(w http.ResponseWriter, r *http.Request) {
	resourceID := pathValue(r, "resource_id")
	if resourceID == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "resource_id path parameter is required")
		return
	}

	resource, err := h.resources.GetResource(r.Context(), resourceID)
	if err != nil {
		writeResourceError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"resource": resource,
	})
}

func writeResourceError(w http.ResponseWriter, err error) {
	if errors.Is(err, irods.ErrPermissionDenied) {
		writeError(w, http.StatusForbidden, "permission_denied", err.Error())
		return
	}
	if errors.Is(err, irods.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not_found", err.Error())
		return
	}
	writeError(w, http.StatusInternalServerError, "internal_error", err.Error())
}
