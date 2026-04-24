package httpapi

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/michael-conway/irods-go-rest/internal/domain"
	"github.com/michael-conway/irods-go-rest/internal/irods"
)

func (h *Handler) getHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status":      "ok",
		"service":     "irods-go-rest",
		"version":     "1.0.0",
		"description": "iRODS REST API service",
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

func (h *Handler) getObjectByPath(w http.ResponseWriter, r *http.Request) {
	objectPath := queryPath(r)
	if objectPath == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "path query parameter is required")
		return
	}

	object, err := h.catalog.GetObjectByPath(r.Context(), objectPath)
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

func (h *Handler) headObjectContentByPath(w http.ResponseWriter, r *http.Request) {
	h.serveObjectContentByPath(w, r, true)
}

func (h *Handler) getObjectContentByPath(w http.ResponseWriter, r *http.Request) {
	h.serveObjectContentByPath(w, r, false)
}

func (h *Handler) serveObjectContentByPath(w http.ResponseWriter, r *http.Request, headOnly bool) {
	objectPath := queryPath(r)
	if objectPath == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "path query parameter is required")
		return
	}

	content, err := h.catalog.GetObjectContentByPath(r.Context(), objectPath)
	if err != nil {
		if errors.Is(err, irods.ErrNotFound) {
			writeError(w, http.StatusNotFound, "not_found", err.Error())
			return
		}

		writeError(w, http.StatusInternalServerError, "internal_error", err.Error())
		return
	}

	status, contentRange, start, end, err := resolveByteRange(r.Header.Get("Range"), content)
	if err != nil {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", content.Size))
		writeError(w, http.StatusRequestedRangeNotSatisfiable, "invalid_range", err.Error())
		return
	}

	w.Header().Set("Content-Type", content.ContentType)
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Length", strconv.FormatInt(end-start, 10))
	if contentRange != "" {
		w.Header().Set("Content-Range", contentRange)
	}
	w.WriteHeader(status)

	if headOnly {
		return
	}

	_, _ = w.Write(content.Data[start:end])
}

func queryPath(r *http.Request) string {
	return strings.TrimSpace(r.URL.Query().Get("path"))
}

func resolveByteRange(rangeHeader string, content domain.ObjectContent) (int, string, int64, int64, error) {
	size := content.Size
	if size < 0 {
		return 0, "", 0, 0, fmt.Errorf("invalid content size")
	}

	if strings.TrimSpace(rangeHeader) == "" {
		return http.StatusOK, "", 0, size, nil
	}

	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(rangeHeader)), "bytes=") {
		return 0, "", 0, 0, fmt.Errorf("unsupported range unit")
	}

	spec := strings.TrimSpace(rangeHeader[len("bytes="):])
	if spec == "" || strings.Contains(spec, ",") {
		return 0, "", 0, 0, fmt.Errorf("only a single byte range is supported")
	}

	parts := strings.SplitN(spec, "-", 2)
	if len(parts) != 2 {
		return 0, "", 0, 0, fmt.Errorf("invalid byte range")
	}

	if strings.TrimSpace(parts[0]) == "" {
		return 0, "", 0, 0, fmt.Errorf("suffix byte ranges are not supported")
	}

	start, err := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
	if err != nil || start < 0 || start >= size {
		return 0, "", 0, 0, fmt.Errorf("invalid byte range start")
	}

	endExclusive := size
	if strings.TrimSpace(parts[1]) != "" {
		endInclusive, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
		if err != nil || endInclusive < start {
			return 0, "", 0, 0, fmt.Errorf("invalid byte range end")
		}
		if endInclusive >= size {
			endInclusive = size - 1
		}
		endExclusive = endInclusive + 1
	}

	return http.StatusPartialContent, fmt.Sprintf("bytes %d-%d/%d", start, endExclusive-1, size), start, endExclusive, nil
}
