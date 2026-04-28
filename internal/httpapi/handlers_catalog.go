package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

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

	verboseLevel, err := queryVerboseLevel(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	object, err := h.paths.GetPath(r.Context(), objectPath, irods.PathLookupOptions{VerboseLevel: verboseLevel})
	if err != nil {
		if errors.Is(err, irods.ErrNotFound) {
			writeError(w, http.StatusNotFound, "not_found", err.Error())
			return
		}
		if errors.Is(err, irods.ErrPermissionDenied) {
			writeError(w, http.StatusForbidden, "permission_denied", err.Error())
			return
		}

		writeError(w, http.StatusInternalServerError, "internal_error", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, pathEntryResponse(r, object))
}

func (h *Handler) deletePath(w http.ResponseWriter, r *http.Request) {
	objectPath := queryIRODSPath(r)
	if objectPath == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "irods_path query parameter is required")
		return
	}

	force, err := queryForceFlag(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	if err := h.paths.DeletePath(r.Context(), objectPath, force); err != nil {
		if errors.Is(err, irods.ErrConflict) {
			writeError(w, http.StatusConflict, "conflict", err.Error())
			return
		}
		writePathError(w, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) postPathMove(w http.ResponseWriter, r *http.Request) {
	objectPath := queryIRODSPath(r)
	if objectPath == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "irods_path query parameter is required")
		return
	}

	var request struct {
		NewName string `json:"new_name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must be valid JSON")
		return
	}
	if strings.TrimSpace(request.NewName) == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "new_name is required")
		return
	}

	renamed, err := h.paths.RenamePath(r.Context(), objectPath, request.NewName)
	if err != nil {
		lowerErr := strings.ToLower(err.Error())
		if strings.Contains(lowerErr, "new_name") {
			writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		writePathError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, pathEntryResponse(r, renamed))
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
		if errors.Is(err, irods.ErrPermissionDenied) {
			writeError(w, http.StatusForbidden, "permission_denied", err.Error())
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
		"irods_path":    objectPath,
		"path_segments": buildPathSegments(objectPath),
		"children":      mappedChildren,
	})
}

func (h *Handler) postPathChildren(w http.ResponseWriter, r *http.Request) {
	objectPath := queryIRODSPath(r)
	if objectPath == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "irods_path query parameter is required")
		return
	}

	var request struct {
		ChildName string `json:"child_name"`
		Kind      string `json:"kind"`
		Mkdirs    bool   `json:"mkdirs"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must be valid JSON")
		return
	}
	if strings.TrimSpace(request.ChildName) == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "child_name is required")
		return
	}
	switch strings.TrimSpace(request.Kind) {
	case "collection", "data_object":
	default:
		writeError(w, http.StatusBadRequest, "invalid_request", "kind must be collection or data_object")
		return
	}
	if request.Mkdirs && request.Kind != "collection" {
		writeError(w, http.StatusBadRequest, "invalid_request", "mkdirs is only supported for collection creation")
		return
	}

	created, err := h.paths.CreatePathChild(r.Context(), objectPath, irods.PathCreateOptions{
		ChildName: request.ChildName,
		Kind:      request.Kind,
		Mkdirs:    request.Mkdirs,
	})
	if err != nil {
		lowerErr := strings.ToLower(err.Error())
		if strings.Contains(lowerErr, "child_name") || strings.Contains(lowerErr, "kind must be") || strings.Contains(lowerErr, "mkdirs is only supported") {
			writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		writePathError(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, pathEntryResponse(r, created))
}

func (h *Handler) getPathAVUs(w http.ResponseWriter, r *http.Request) {
	objectPath := queryIRODSPath(r)
	if objectPath == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "irods_path query parameter is required")
		return
	}

	options, err := queryAVUListOptions(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	metadata, err := h.paths.GetPathMetadata(r.Context(), objectPath)
	if err != nil {
		if errors.Is(err, irods.ErrNotFound) {
			writeError(w, http.StatusNotFound, "not_found", err.Error())
			return
		}
		if errors.Is(err, irods.ErrPermissionDenied) {
			writeError(w, http.StatusForbidden, "permission_denied", err.Error())
			return
		}

		writeError(w, http.StatusInternalServerError, "internal_error", err.Error())
		return
	}

	metadata, total := applyAVUListOptions(metadata, options)

	writeJSON(w, http.StatusOK, map[string]any{
		"irods_path":    objectPath,
		"path_segments": buildPathSegments(objectPath),
		"links":         pathLinksForEntry(objectPath, ""),
		"avus":          avuMetadataResponseList(r, objectPath, metadata),
		"count":         len(metadata),
		"total":         total,
		"offset":        options.Offset,
		"limit":         options.Limit,
	})
}

func (h *Handler) postPathAVU(w http.ResponseWriter, r *http.Request) {
	objectPath := queryIRODSPath(r)
	if objectPath == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "irods_path query parameter is required")
		return
	}

	var request struct {
		Attrib string `json:"attrib"`
		Value  string `json:"value"`
		Unit   string `json:"unit"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must be valid JSON")
		return
	}
	if fields := avuValidationFields(request.Attrib, request.Value); len(fields) > 0 {
		writeValidationError(w, http.StatusBadRequest, "invalid_request", "AVU request validation failed", fields)
		return
	}

	created, err := h.paths.AddPathMetadata(r.Context(), objectPath, request.Attrib, request.Value, request.Unit)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "attrib and value are required") {
			writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		writePathError(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"irods_path":    objectPath,
		"path_segments": buildPathSegments(objectPath),
		"avu":           avuMetadataResponse(r, objectPath, created),
	})
}

func (h *Handler) putPathAVU(w http.ResponseWriter, r *http.Request) {
	objectPath := queryIRODSPath(r)
	if objectPath == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "irods_path query parameter is required")
		return
	}

	avuID := pathValue(r, "avu_id")
	if avuID == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "avu_id path parameter is required")
		return
	}

	var request struct {
		Attrib string `json:"attrib"`
		Value  string `json:"value"`
		Unit   string `json:"unit"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must be valid JSON")
		return
	}
	if fields := avuValidationFields(request.Attrib, request.Value); len(fields) > 0 {
		writeValidationError(w, http.StatusBadRequest, "invalid_request", "AVU request validation failed", fields)
		return
	}

	updated, err := h.paths.UpdatePathMetadata(r.Context(), objectPath, avuID, request.Attrib, request.Value, request.Unit)
	if err != nil {
		lowerErr := strings.ToLower(err.Error())
		if strings.Contains(lowerErr, "attrib and value are required") || strings.Contains(lowerErr, "invalid avu id") {
			writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		writePathError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"irods_path":    objectPath,
		"path_segments": buildPathSegments(objectPath),
		"avu":           avuMetadataResponse(r, objectPath, updated),
	})
}

func (h *Handler) deletePathAVU(w http.ResponseWriter, r *http.Request) {
	objectPath := queryIRODSPath(r)
	if objectPath == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "irods_path query parameter is required")
		return
	}

	avuID := pathValue(r, "avu_id")
	if avuID == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "avu_id path parameter is required")
		return
	}

	if err := h.paths.DeletePathMetadata(r.Context(), objectPath, avuID); err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "invalid avu id") {
			writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		writePathError(w, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) getPathChecksum(w http.ResponseWriter, r *http.Request) {
	objectPath := queryIRODSPath(r)
	if objectPath == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "irods_path query parameter is required")
		return
	}

	checksum, err := h.paths.GetPathChecksum(r.Context(), objectPath)
	if err != nil {
		writePathError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, pathChecksumResponse(objectPath, checksum))
}

func (h *Handler) postPathChecksum(w http.ResponseWriter, r *http.Request) {
	objectPath := queryIRODSPath(r)
	if objectPath == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "irods_path query parameter is required")
		return
	}

	checksum, err := h.paths.ComputePathChecksum(r.Context(), objectPath)
	if err != nil {
		writePathError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, pathChecksumResponse(objectPath, checksum))
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
		if errors.Is(err, irods.ErrPermissionDenied) {
			writeError(w, http.StatusForbidden, "permission_denied", err.Error())
			return
		}

		writeError(w, http.StatusInternalServerError, "internal_error", err.Error())
		return
	}
	defer func() {
		if content.Reader != nil {
			_ = content.Reader.Close()
		}
	}()

	status, contentRange, start, end, err := resolveByteRange(r.Header.Get("Range"), content)
	if err != nil {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", content.Size))
		writeError(w, http.StatusRequestedRangeNotSatisfiable, "invalid_range", err.Error())
		return
	}

	w.Header().Set("Content-Type", content.ContentType)
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Length", strconv.FormatInt(end-start, 10))
	if content.FileName != "" {
		w.Header().Set("Content-Disposition", contentDispositionHeader(content.FileName))
	}
	if content.UpdatedAt != nil {
		w.Header().Set("Last-Modified", content.UpdatedAt.UTC().Format(http.TimeFormat))
	}
	if content.Checksum != nil && strings.TrimSpace(content.Checksum.Checksum) != "" {
		w.Header().Set("ETag", strconv.Quote(content.Checksum.Checksum))
	}
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if contentRange != "" {
		w.Header().Set("Content-Range", contentRange)
	}
	w.WriteHeader(status)

	if headOnly {
		return
	}

	if content.Reader == nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "missing object reader")
		return
	}

	reader := io.NewSectionReader(content.Reader, start, end-start)
	if _, err := io.CopyN(w, reader, end-start); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, http.StatusInternalServerError, "internal_error", err.Error())
	}
}

func queryIRODSPath(r *http.Request) string {
	return strings.TrimSpace(r.URL.Query().Get("irods_path"))
}

func queryTicketID(r *http.Request) string {
	return strings.TrimSpace(r.URL.Query().Get("ticket_id"))
}

func queryForceFlag(r *http.Request) (bool, error) {
	raw := strings.TrimSpace(r.URL.Query().Get("force"))
	if raw == "" {
		return false, nil
	}

	switch strings.ToLower(raw) {
	case "1", "true", "yes", "on":
		return true, nil
	case "0", "false", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("force query parameter must be a boolean")
	}
}

func writePathError(w http.ResponseWriter, err error) {
	if errors.Is(err, irods.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not_found", err.Error())
		return
	}
	if errors.Is(err, irods.ErrPermissionDenied) {
		writeError(w, http.StatusForbidden, "permission_denied", err.Error())
		return
	}
	if errors.Is(err, irods.ErrConflict) {
		writeError(w, http.StatusConflict, "conflict", err.Error())
		return
	}

	writeError(w, http.StatusInternalServerError, "internal_error", err.Error())
}

func queryVerboseLevel(r *http.Request) (int, error) {
	raw := strings.TrimSpace(r.URL.Query().Get("verbose"))
	if raw == "" {
		return 0, nil
	}

	switch strings.ToLower(raw) {
	case "0", "false", "off", "none":
		return 0, nil
	case "1", "true", "on", "l", "long":
		return 1, nil
	case "2", "ll", "l2", "very_long", "very-long", "full":
		return 2, nil
	default:
		return 0, fmt.Errorf("verbose query parameter must be one of 0, 1, 2, true, false, long, or very_long")
	}
}

type avuListOptions struct {
	Attrib   string
	Sort     string
	Order    string
	Limit    int
	Offset   int
	hasLimit bool
}

func queryAVUListOptions(r *http.Request) (avuListOptions, error) {
	query := r.URL.Query()
	options := avuListOptions{
		Attrib: strings.TrimSpace(query.Get("attrib")),
		Sort:   strings.TrimSpace(query.Get("sort")),
		Order:  strings.ToLower(strings.TrimSpace(query.Get("order"))),
	}

	if options.Sort != "" {
		switch options.Sort {
		case "id", "attrib", "value", "unit", "created_at", "updated_at":
		default:
			return avuListOptions{}, fmt.Errorf("sort query parameter must be one of id, attrib, value, unit, created_at, or updated_at")
		}
	}

	if options.Order == "" {
		options.Order = "asc"
	}
	switch options.Order {
	case "asc", "desc":
	default:
		return avuListOptions{}, fmt.Errorf("order query parameter must be asc or desc")
	}

	if rawLimit := strings.TrimSpace(query.Get("limit")); rawLimit != "" {
		limit, err := strconv.Atoi(rawLimit)
		if err != nil || limit < 1 || limit > 1000 {
			return avuListOptions{}, fmt.Errorf("limit query parameter must be an integer from 1 through 1000")
		}
		options.Limit = limit
		options.hasLimit = true
	}

	if rawOffset := strings.TrimSpace(query.Get("offset")); rawOffset != "" {
		offset, err := strconv.Atoi(rawOffset)
		if err != nil || offset < 0 {
			return avuListOptions{}, fmt.Errorf("offset query parameter must be a non-negative integer")
		}
		options.Offset = offset
	}

	return options, nil
}

func applyAVUListOptions(metadata []domain.AVUMetadata, options avuListOptions) ([]domain.AVUMetadata, int) {
	filtered := metadata
	if options.Attrib != "" {
		filtered = make([]domain.AVUMetadata, 0, len(metadata))
		for _, avu := range metadata {
			if avu.Attrib == options.Attrib {
				filtered = append(filtered, avu)
			}
		}
	}

	if options.Sort != "" {
		sort.SliceStable(filtered, func(i, j int) bool {
			cmp := compareAVUMetadata(filtered[i], filtered[j], options.Sort)
			if options.Order == "desc" {
				return cmp > 0
			}
			return cmp < 0
		})
	}

	total := len(filtered)
	if options.Offset >= total {
		return nil, total
	}

	start := options.Offset
	end := total
	if options.hasLimit && start+options.Limit < end {
		end = start + options.Limit
	}

	return filtered[start:end], total
}

func compareAVUMetadata(left domain.AVUMetadata, right domain.AVUMetadata, field string) int {
	switch field {
	case "id":
		return strings.Compare(left.ID, right.ID)
	case "attrib":
		return strings.Compare(left.Attrib, right.Attrib)
	case "value":
		return strings.Compare(left.Value, right.Value)
	case "unit":
		return strings.Compare(left.Unit, right.Unit)
	case "created_at":
		return compareOptionalTime(left.CreatedAt, right.CreatedAt)
	case "updated_at":
		return compareOptionalTime(left.UpdatedAt, right.UpdatedAt)
	default:
		return 0
	}
}

func compareOptionalTime(left *time.Time, right *time.Time) int {
	switch {
	case left == nil && right == nil:
		return 0
	case left == nil:
		return -1
	case right == nil:
		return 1
	case left.Before(*right):
		return -1
	case left.After(*right):
		return 1
	default:
		return 0
	}
}

func avuValidationFields(attrib string, value string) map[string]string {
	fields := map[string]string{}
	if strings.TrimSpace(attrib) == "" {
		fields["attrib"] = "attribute is required"
	}
	if strings.TrimSpace(value) == "" {
		fields["value"] = "value is required"
	}
	if len(fields) == 0 {
		return nil
	}

	return fields
}

func pathEntryResponse(r *http.Request, entry domain.PathEntry) domain.PathEntry {
	entry.Links = pathLinksForEntry(entry.Path, entry.Kind)
	entry.Parent = buildParentLink(r, entry.Path)
	entry.PathSegments = buildPathSegments(entry.Path)
	return entry
}

func avuMetadataResponseList(r *http.Request, irodsPath string, metadata []domain.AVUMetadata) []domain.AVUMetadata {
	if len(metadata) == 0 {
		return nil
	}

	mapped := make([]domain.AVUMetadata, 0, len(metadata))
	for _, avu := range metadata {
		mapped = append(mapped, avuMetadataResponse(r, irodsPath, avu))
	}
	return mapped
}

func avuMetadataResponse(r *http.Request, irodsPath string, avu domain.AVUMetadata) domain.AVUMetadata {
	avu.Links = avuLinksForEntry(irodsPath, avu.ID)
	return avu
}

func pathChecksumResponse(irodsPath string, checksum domain.PathChecksum) map[string]any {
	return map[string]any{
		"irods_path":    irodsPath,
		"path_segments": buildPathSegments(irodsPath),
		"checksum":      checksum.Checksum,
		"type":          checksum.Type,
	}
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

func pathLinksForEntry(irodsPath string, kind string) *domain.PathLinks {
	irodsPath = strings.TrimSpace(irodsPath)
	if irodsPath == "" {
		return nil
	}

	avuPath := "/api/v1/path/avu?irods_path=" + url.QueryEscape(irodsPath)
	links := &domain.PathLinks{
		AVUs: &domain.ActionLink{
			Href:   avuPath,
			Method: http.MethodGet,
		},
		CreateAVU: &domain.ActionLink{
			Href:   avuPath,
			Method: http.MethodPost,
		},
		CreateTicket: &domain.ActionLink{
			Href:   "/api/v1/path/ticket?irods_path=" + url.QueryEscape(irodsPath),
			Method: http.MethodPost,
		},
	}

	if kind == "collection" {
		createChildrenPath := "/api/v1/path/children?irods_path=" + url.QueryEscape(irodsPath)
		links.CreateChildCollection = &domain.ActionLink{
			Href:   createChildrenPath,
			Method: http.MethodPost,
		}
		links.CreateChildDataObject = &domain.ActionLink{
			Href:   createChildrenPath,
			Method: http.MethodPost,
		}
	}

	return links
}

func avuLinksForEntry(irodsPath string, avuID string) *domain.AVULinks {
	irodsPath = strings.TrimSpace(irodsPath)
	avuID = strings.TrimSpace(avuID)
	if irodsPath == "" || avuID == "" {
		return nil
	}

	pathWithID := "/api/v1/path/avu/" + url.PathEscape(avuID) + "?irods_path=" + url.QueryEscape(irodsPath)
	return &domain.AVULinks{
		Update: &domain.ActionLink{
			Href:   pathWithID,
			Method: http.MethodPut,
		},
		Delete: &domain.ActionLink{
			Href:   pathWithID,
			Method: http.MethodDelete,
		},
	}
}

func buildPathSegments(irodsPath string) []domain.PathSegmentLink {
	irodsPath = strings.TrimSpace(irodsPath)
	if irodsPath == "" {
		return nil
	}

	cleaned := path.Clean(irodsPath)
	if cleaned == "." {
		return nil
	}

	if cleaned == "/" {
		return []domain.PathSegmentLink{{
			DisplayName: "/",
			IRODSPath:   "/",
			Href:        "/api/v1/path?irods_path=%2F",
		}}
	}

	parts := strings.Split(strings.TrimPrefix(cleaned, "/"), "/")
	segments := make([]domain.PathSegmentLink, 0, len(parts))
	currentPath := ""

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		currentPath += "/" + part
		segments = append(segments, domain.PathSegmentLink{
			DisplayName: part,
			IRODSPath:   currentPath,
			Href:        "/api/v1/path?irods_path=" + url.QueryEscape(currentPath),
		})
	}

	return segments
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

func contentDispositionHeader(fileName string) string {
	fileName = strings.TrimSpace(fileName)
	if fileName == "" {
		return `attachment`
	}

	safeName := strings.NewReplacer("\\", "_", "\"", "_", "\r", "_", "\n", "_").Replace(fileName)
	return fmt.Sprintf(`attachment; filename="%s"; filename*=UTF-8''%s`, safeName, url.QueryEscape(fileName))
}
