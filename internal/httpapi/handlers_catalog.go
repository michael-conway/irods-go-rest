package httpapi

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
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

func (h *Handler) getPath(w http.ResponseWriter, r *http.Request) {
	objectPath := queryIRODSPath(r)
	if objectPath == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "irods_path query parameter is required")
		return
	}

	object, err := h.paths.GetPath(r.Context(), objectPath)
	if err != nil {
		if errors.Is(err, irods.ErrNotFound) {
			writeError(w, http.StatusNotFound, "not_found", err.Error())
			return
		}

		writeError(w, http.StatusInternalServerError, "internal_error", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, pathEntryResponse(r, object))
}

func (h *Handler) getPathChildren(w http.ResponseWriter, r *http.Request) {
	objectPath := queryIRODSPath(r)
	if objectPath == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "irods_path query parameter is required")
		return
	}

	children, err := h.paths.GetPathChildren(r.Context(), objectPath)
	if err != nil {
		if errors.Is(err, irods.ErrNotFound) {
			writeError(w, http.StatusNotFound, "not_found", err.Error())
			return
		}

		writeError(w, http.StatusInternalServerError, "internal_error", err.Error())
		return
	}

	mappedChildren := make([]domain.PathEntry, 0, len(children))
	for _, child := range children {
		mappedChildren = append(mappedChildren, pathEntryResponse(r, child))
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"irods_path": objectPath,
		"children":   mappedChildren,
	})
}

func (h *Handler) headPathContents(w http.ResponseWriter, r *http.Request) {
	h.servePathContents(w, r, true)
}

func (h *Handler) getPathContents(w http.ResponseWriter, r *http.Request) {
	h.servePathContents(w, r, false)
}

func (h *Handler) servePathContents(w http.ResponseWriter, r *http.Request, headOnly bool) {
	objectPath := queryIRODSPath(r)
	if objectPath == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "irods_path query parameter is required")
		return
	}

	content, err := h.paths.GetObjectContentByPath(r.Context(), objectPath)
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

func queryIRODSPath(r *http.Request) string {
	return strings.TrimSpace(r.URL.Query().Get("irods_path"))
}

func pathEntryResponse(r *http.Request, entry domain.PathEntry) domain.PathEntry {
	entry.Parent = buildParentLink(r, entry.Path)
	return entry
}

func buildParentLink(r *http.Request, irodsPath string) *domain.ParentLink {
	irodsPath = strings.TrimSpace(irodsPath)
	if irodsPath == "" || irodsPath == "/" {
		return nil
	}

	cleaned := path.Clean(irodsPath)
	if cleaned == "." || cleaned == "/" {
		return nil
	}

	parentPath := path.Dir(cleaned)
	if parentPath == "." || parentPath == "" || parentPath == cleaned {
		return nil
	}

	return &domain.ParentLink{
		IRODSPath: parentPath,
		Href:      "/api/v1/path?irods_path=" + url.QueryEscape(parentPath),
	}
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
