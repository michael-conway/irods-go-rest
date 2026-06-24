package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/michael-conway/irods-go-rest/internal/domain"
	"github.com/michael-conway/irods-go-rest/internal/irods"
)

type principalAVURequest struct {
	Attrib string `json:"attrib"`
	Value  string `json:"value"`
	Unit   string `json:"unit"`
}

type principalAVUHandlerConfig struct {
	PathParam    string
	DisplayName  string
	BasePath     string
	ResponseName string
	List         func(name string, zone string) ([]domain.AVUMetadata, error)
	Add          func(name string, zone string, request principalAVURequest) (domain.AVUMetadata, error)
	Update       func(name string, zone string, avuID string, request principalAVURequest) (domain.AVUMetadata, error)
	Delete       func(name string, zone string, avuID string) error
	WriteError   func(err error)
}

func (h *Handler) getUserAVUs(w http.ResponseWriter, r *http.Request) {
	h.getPrincipalAVUs(w, r, principalAVUHandlerConfig{
		PathParam:    "user_name",
		DisplayName:  "user_name",
		BasePath:     "/api/v1/user",
		ResponseName: "user_name",
		List: func(name string, zone string) ([]domain.AVUMetadata, error) {
			return h.users.GetUserMetadata(r.Context(), name, zone)
		},
		WriteError: func(err error) {
			writeUserError(w, err)
		},
	})
}

func (h *Handler) postUserAVU(w http.ResponseWriter, r *http.Request) {
	h.postPrincipalAVU(w, r, principalAVUHandlerConfig{
		PathParam:    "user_name",
		DisplayName:  "user_name",
		BasePath:     "/api/v1/user",
		ResponseName: "user_name",
		Add: func(name string, zone string, request principalAVURequest) (domain.AVUMetadata, error) {
			return h.users.AddUserMetadata(r.Context(), name, zone, request.Attrib, request.Value, request.Unit)
		},
	})
}

func (h *Handler) putUserAVU(w http.ResponseWriter, r *http.Request) {
	h.putPrincipalAVU(w, r, principalAVUHandlerConfig{
		PathParam:    "user_name",
		DisplayName:  "user_name",
		BasePath:     "/api/v1/user",
		ResponseName: "user_name",
		Update: func(name string, zone string, avuID string, request principalAVURequest) (domain.AVUMetadata, error) {
			return h.users.UpdateUserMetadata(r.Context(), name, zone, avuID, request.Attrib, request.Value, request.Unit)
		},
	})
}

func (h *Handler) deleteUserAVU(w http.ResponseWriter, r *http.Request) {
	h.deletePrincipalAVU(w, r, principalAVUHandlerConfig{
		PathParam:   "user_name",
		DisplayName: "user_name",
		Delete: func(name string, zone string, avuID string) error {
			return h.users.DeleteUserMetadata(r.Context(), name, zone, avuID)
		},
	})
}

func (h *Handler) getUserGroupAVUs(w http.ResponseWriter, r *http.Request) {
	h.getPrincipalAVUs(w, r, principalAVUHandlerConfig{
		PathParam:    "group_name",
		DisplayName:  "group_name",
		BasePath:     "/api/v1/usergroup",
		ResponseName: "group_name",
		List: func(name string, zone string) ([]domain.AVUMetadata, error) {
			return h.userGroups.GetUserGroupMetadata(r.Context(), name, zone)
		},
		WriteError: func(err error) {
			writeUserGroupError(w, err)
		},
	})
}

func (h *Handler) postUserGroupAVU(w http.ResponseWriter, r *http.Request) {
	h.postPrincipalAVU(w, r, principalAVUHandlerConfig{
		PathParam:    "group_name",
		DisplayName:  "group_name",
		BasePath:     "/api/v1/usergroup",
		ResponseName: "group_name",
		Add: func(name string, zone string, request principalAVURequest) (domain.AVUMetadata, error) {
			return h.userGroups.AddUserGroupMetadata(r.Context(), name, zone, request.Attrib, request.Value, request.Unit)
		},
	})
}

func (h *Handler) putUserGroupAVU(w http.ResponseWriter, r *http.Request) {
	h.putPrincipalAVU(w, r, principalAVUHandlerConfig{
		PathParam:    "group_name",
		DisplayName:  "group_name",
		BasePath:     "/api/v1/usergroup",
		ResponseName: "group_name",
		Update: func(name string, zone string, avuID string, request principalAVURequest) (domain.AVUMetadata, error) {
			return h.userGroups.UpdateUserGroupMetadata(r.Context(), name, zone, avuID, request.Attrib, request.Value, request.Unit)
		},
	})
}

func (h *Handler) deleteUserGroupAVU(w http.ResponseWriter, r *http.Request) {
	h.deletePrincipalAVU(w, r, principalAVUHandlerConfig{
		PathParam:   "group_name",
		DisplayName: "group_name",
		Delete: func(name string, zone string, avuID string) error {
			return h.userGroups.DeleteUserGroupMetadata(r.Context(), name, zone, avuID)
		},
	})
}

func (h *Handler) getPrincipalAVUs(w http.ResponseWriter, r *http.Request, config principalAVUHandlerConfig) {
	name := pathValue(r, config.PathParam)
	if name == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", config.DisplayName+" path parameter is required")
		return
	}
	options, err := queryAVUListOptions(r)
	if err != nil {
		writeErrorFromErr(w, r, http.StatusBadRequest, "invalid_request", err)
		return
	}

	zone := h.userZoneFromRequest(r)
	metadata, err := config.List(name, zone)
	if err != nil {
		config.WriteError(err)
		return
	}
	metadata, total := applyAVUListOptions(metadata, options)

	payload := principalAVUCollectionPayload(config, name, zone, metadata)
	payload["count"] = len(metadata)
	payload["total"] = total
	payload["offset"] = options.Offset
	payload["limit"] = options.Limit
	writeJSON(w, http.StatusOK, payload)
}

func (h *Handler) postPrincipalAVU(w http.ResponseWriter, r *http.Request, config principalAVUHandlerConfig) {
	name := pathValue(r, config.PathParam)
	if name == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", config.DisplayName+" path parameter is required")
		return
	}
	request, ok := principalAVURequestFromRequest(w, r)
	if !ok {
		return
	}

	zone := h.userZoneFromRequest(r)
	created, err := config.Add(name, zone, request)
	if err != nil {
		writePrincipalAVUError(w, r, err)
		return
	}

	writeJSON(w, http.StatusCreated, principalAVUSinglePayload(config, name, zone, created))
}

func (h *Handler) putPrincipalAVU(w http.ResponseWriter, r *http.Request, config principalAVUHandlerConfig) {
	name := pathValue(r, config.PathParam)
	if name == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", config.DisplayName+" path parameter is required")
		return
	}
	avuID := pathValue(r, "avu_id")
	if avuID == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "avu_id path parameter is required")
		return
	}
	request, ok := principalAVURequestFromRequest(w, r)
	if !ok {
		return
	}

	zone := h.userZoneFromRequest(r)
	updated, err := config.Update(name, zone, avuID, request)
	if err != nil {
		writePrincipalAVUError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, principalAVUSinglePayload(config, name, zone, updated))
}

func (h *Handler) deletePrincipalAVU(w http.ResponseWriter, r *http.Request, config principalAVUHandlerConfig) {
	name := pathValue(r, config.PathParam)
	if name == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", config.DisplayName+" path parameter is required")
		return
	}
	avuID := pathValue(r, "avu_id")
	if avuID == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "avu_id path parameter is required")
		return
	}

	if err := config.Delete(name, h.userZoneFromRequest(r), avuID); err != nil {
		writePrincipalAVUError(w, r, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func principalAVUCollectionPayload(config principalAVUHandlerConfig, name string, zone string, metadata []domain.AVUMetadata) map[string]any {
	return map[string]any{
		config.ResponseName: name,
		"zone":              zone,
		"links": map[string]domain.ActionLink{
			"self": {
				Href:   principalAVUCollectionHref(config.BasePath, name, zone),
				Method: http.MethodGet,
			},
			"create": {
				Href:   principalAVUCollectionHref(config.BasePath, name, zone),
				Method: http.MethodPost,
			},
		},
		"avus": principalAVUResponseList(config.BasePath, name, zone, metadata),
	}
}

func principalAVUSinglePayload(config principalAVUHandlerConfig, name string, zone string, avu domain.AVUMetadata) map[string]any {
	return map[string]any{
		config.ResponseName: name,
		"zone":              zone,
		"avu":               principalAVUResponse(config.BasePath, name, zone, avu),
	}
}

func principalAVURequestFromRequest(w http.ResponseWriter, r *http.Request) (principalAVURequest, bool) {
	var request principalAVURequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must be valid JSON")
		return principalAVURequest{}, false
	}
	if fields := avuValidationFields(request.Attrib, request.Value); len(fields) > 0 {
		writeValidationError(w, http.StatusBadRequest, "invalid_request", "AVU request validation failed", fields)
		return principalAVURequest{}, false
	}
	return request, true
}

func writePrincipalAVUError(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, irods.ErrInvalidRequest) {
		writeErrorFromErr(w, r, http.StatusBadRequest, "invalid_request", err)
		return
	}
	if errors.Is(err, irods.ErrNotFound) {
		writeErrorFromErr(w, r, http.StatusNotFound, "not_found", err)
		return
	}
	if errors.Is(err, irods.ErrPermissionDenied) {
		writeErrorFromErr(w, r, http.StatusForbidden, "permission_denied", err)
		return
	}
	writeErrorFromErr(w, r, http.StatusInternalServerError, "internal_error", err)
}

func principalAVUResponseList(basePath string, name string, zone string, metadata []domain.AVUMetadata) []domain.AVUMetadata {
	if len(metadata) == 0 {
		return nil
	}

	mapped := make([]domain.AVUMetadata, 0, len(metadata))
	for _, avu := range metadata {
		mapped = append(mapped, principalAVUResponse(basePath, name, zone, avu))
	}
	return mapped
}

func principalAVUResponse(basePath string, name string, zone string, avu domain.AVUMetadata) domain.AVUMetadata {
	avu.Links = principalAVULinks(basePath, name, zone, avu.ID)
	return avu
}

func principalAVULinks(basePath string, name string, zone string, avuID string) *domain.AVULinks {
	href := principalAVUItemHref(basePath, name, zone, avuID)
	if href == "" {
		return nil
	}
	return &domain.AVULinks{
		Update: &domain.ActionLink{
			Href:   href,
			Method: http.MethodPut,
		},
		Delete: &domain.ActionLink{
			Href:   href,
			Method: http.MethodDelete,
		},
	}
}

func principalAVUCollectionHref(basePath string, name string, zone string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}

	query := url.Values{}
	if strings.TrimSpace(zone) != "" {
		query.Set("zone", strings.TrimSpace(zone))
	}

	href := strings.TrimRight(basePath, "/") + "/" + url.PathEscape(name) + "/avu"
	if encoded := query.Encode(); encoded != "" {
		href += "?" + encoded
	}
	return href
}

func principalAVUItemHref(basePath string, name string, zone string, avuID string) string {
	avuID = strings.TrimSpace(avuID)
	if avuID == "" {
		return ""
	}
	collectionHref := principalAVUCollectionHref(basePath, name, zone)
	if collectionHref == "" {
		return ""
	}

	parts := strings.SplitN(collectionHref, "?", 2)
	href := parts[0] + "/" + url.PathEscape(avuID)
	if len(parts) == 2 {
		href += "?" + parts[1]
	}
	return href
}
