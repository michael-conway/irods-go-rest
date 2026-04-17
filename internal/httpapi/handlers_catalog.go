package httpapi

import (
	"errors"
	"net/http"

	"github.com/michael-conway/irods-go-rest/internal/irods"
)

func (h *Handler) getHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status":      "ok",
		"service":     h.cfg.ServiceName,
		"environment": h.cfg.Environment,
	})
}

func (h *Handler) getObject(w http.ResponseWriter, r *http.Request) {
	objectID := pathValue(r, "object_id")
	if objectID == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "object_id is required")
		return
	}

	object, err := h.catalog.GetObject(r.Context(), objectID)
	if err != nil {
		if errors.Is(err, irods.ErrNotFound) {
			writeError(w, http.StatusNotFound, "not_found", err.Error())
			return
		}

		writeError(w, http.StatusInternalServerError, "internal_error", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, object)
}

func (h *Handler) getCollection(w http.ResponseWriter, r *http.Request) {
	collectionID := pathValue(r, "collection_id")
	if collectionID == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "collection_id is required")
		return
	}

	collection, err := h.catalog.GetCollection(r.Context(), collectionID)
	if err != nil {
		if errors.Is(err, irods.ErrNotFound) {
			writeError(w, http.StatusNotFound, "not_found", err.Error())
			return
		}

		writeError(w, http.StatusInternalServerError, "internal_error", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, collection)
}
