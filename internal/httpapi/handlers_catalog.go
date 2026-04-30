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

	irodstypes "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/michael-conway/go-irodsclient-extensions/cmdcues"
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

func (h *Handler) patchPath(w http.ResponseWriter, r *http.Request) {
	h.renamePath(w, r)
}

func (h *Handler) renamePath(w http.ResponseWriter, r *http.Request) {
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

func (h *Handler) getPathReplicas(w http.ResponseWriter, r *http.Request) {
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

	replicas, err := h.paths.GetPathReplicas(r.Context(), objectPath, verboseLevel)
	if err != nil {
		writePathError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"irods_path":    objectPath,
		"path_segments": buildPathSegments(objectPath),
		"replicas":      replicas,
	})
}

func (h *Handler) postPathReplicas(w http.ResponseWriter, r *http.Request) {
	objectPath := queryIRODSPath(r)
	if objectPath == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "irods_path query parameter is required")
		return
	}

	var request struct {
		Resource string `json:"resource"`
		Update   bool   `json:"update"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must be valid JSON")
		return
	}

	if strings.TrimSpace(request.Resource) == "" {
		writeValidationError(w, http.StatusBadRequest, "invalid_request", "replica request validation failed", map[string]string{
			"resource": "resource is required",
		})
		return
	}

	replicas, err := h.paths.CreatePathReplica(r.Context(), objectPath, irods.PathReplicaCreateOptions{
		Resource: request.Resource,
		Update:   request.Update,
	})
	if err != nil {
		lowerErr := strings.ToLower(err.Error())
		if strings.Contains(lowerErr, "resource is required") {
			writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		writePathError(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"irods_path":    objectPath,
		"path_segments": buildPathSegments(objectPath),
		"replicas":      replicas,
	})
}

func (h *Handler) patchPathReplicas(w http.ResponseWriter, r *http.Request) {
	objectPath := queryIRODSPath(r)
	if objectPath == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "irods_path query parameter is required")
		return
	}

	var request struct {
		SourceResource      string `json:"source_resource"`
		DestinationResource string `json:"destination_resource"`
		Update              bool   `json:"update"`
		MinCopies           *int   `json:"min_copies"`
		MinAgeMinutes       *int   `json:"min_age_minutes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must be valid JSON")
		return
	}

	validationErrors := map[string]string{}
	if strings.TrimSpace(request.SourceResource) == "" {
		validationErrors["source_resource"] = "source_resource is required"
	}
	if strings.TrimSpace(request.DestinationResource) == "" {
		validationErrors["destination_resource"] = "destination_resource is required"
	}
	if strings.TrimSpace(request.SourceResource) != "" && strings.TrimSpace(request.SourceResource) == strings.TrimSpace(request.DestinationResource) {
		validationErrors["destination_resource"] = "destination_resource must differ from source_resource"
	}
	if request.MinCopies != nil && *request.MinCopies < 0 {
		validationErrors["min_copies"] = "min_copies must be >= 0"
	}
	if request.MinAgeMinutes != nil && *request.MinAgeMinutes < 0 {
		validationErrors["min_age_minutes"] = "min_age_minutes must be >= 0"
	}
	if len(validationErrors) > 0 {
		writeValidationError(w, http.StatusBadRequest, "invalid_request", "replica request validation failed", validationErrors)
		return
	}

	minCopies, minAgeMinutes := h.replicaTrimDefaults()
	if request.MinCopies != nil {
		minCopies = *request.MinCopies
	}
	if request.MinAgeMinutes != nil {
		minAgeMinutes = *request.MinAgeMinutes
	}

	replicas, err := h.paths.MovePathReplica(r.Context(), objectPath, irods.PathReplicaMoveOptions{
		SourceResource:      request.SourceResource,
		DestinationResource: request.DestinationResource,
		Update:              request.Update,
		MinCopies:           minCopies,
		MinAgeMinutes:       minAgeMinutes,
	})
	if err != nil {
		lowerErr := strings.ToLower(err.Error())
		if strings.Contains(lowerErr, "source_resource is required") || strings.Contains(lowerErr, "destination_resource is required") || strings.Contains(lowerErr, "must differ") {
			writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		writePathError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"irods_path":    objectPath,
		"path_segments": buildPathSegments(objectPath),
		"replicas":      replicas,
	})
}

func (h *Handler) deletePathReplicas(w http.ResponseWriter, r *http.Request) {
	objectPath := queryIRODSPath(r)
	if objectPath == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "irods_path query parameter is required")
		return
	}

	var request struct {
		Resource      string `json:"resource"`
		ReplicaNumber *int64 `json:"replica_number"`
		MinCopies     *int   `json:"min_copies"`
		MinAgeMinutes *int   `json:"min_age_minutes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must be valid JSON")
		return
	}

	if strings.TrimSpace(request.Resource) == "" && request.ReplicaNumber == nil {
		writeValidationError(w, http.StatusBadRequest, "invalid_request", "replica request validation failed", map[string]string{
			"resource":       "resource or replica_number is required",
			"replica_number": "resource or replica_number is required",
		})
		return
	}
	if request.MinCopies != nil && *request.MinCopies < 0 {
		writeValidationError(w, http.StatusBadRequest, "invalid_request", "replica request validation failed", map[string]string{
			"min_copies": "min_copies must be >= 0",
		})
		return
	}
	if request.MinAgeMinutes != nil && *request.MinAgeMinutes < 0 {
		writeValidationError(w, http.StatusBadRequest, "invalid_request", "replica request validation failed", map[string]string{
			"min_age_minutes": "min_age_minutes must be >= 0",
		})
		return
	}

	minCopies, minAgeMinutes := h.replicaTrimDefaults()
	if request.MinCopies != nil {
		minCopies = *request.MinCopies
	}
	if request.MinAgeMinutes != nil {
		minAgeMinutes = *request.MinAgeMinutes
	}

	replicas, err := h.paths.TrimPathReplica(r.Context(), objectPath, irods.PathReplicaTrimOptions{
		Resource:      request.Resource,
		ReplicaIndex:  request.ReplicaNumber,
		MinCopies:     minCopies,
		MinAgeMinutes: minAgeMinutes,
	})
	if err != nil {
		lowerErr := strings.ToLower(err.Error())
		if strings.Contains(lowerErr, "resource or replica_index is required") || strings.Contains(lowerErr, "replica_index") {
			writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		writePathError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"irods_path":    objectPath,
		"path_segments": buildPathSegments(objectPath),
		"replicas":      replicas,
	})
}

func (h *Handler) replicaTrimDefaults() (int, int) {
	minCopies := h.cfg.ReplicaTrimMinCopies
	if minCopies <= 0 {
		minCopies = 1
	}

	minAgeMinutes := h.cfg.ReplicaTrimMinAgeMinutes
	if minAgeMinutes < 0 {
		minAgeMinutes = 0
	}

	return minCopies, minAgeMinutes
}

func (h *Handler) getPathACL(w http.ResponseWriter, r *http.Request) {
	objectPath := queryIRODSPath(r)
	if objectPath == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "irods_path query parameter is required")
		return
	}

	acl, err := h.paths.GetPathACL(r.Context(), objectPath)
	if err != nil {
		writePathError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, pathACLResponse(r, acl))
}

func (h *Handler) postPathACL(w http.ResponseWriter, r *http.Request) {
	objectPath := queryIRODSPath(r)
	if objectPath == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "irods_path query parameter is required")
		return
	}

	var request struct {
		Name        string `json:"name"`
		Zone        string `json:"zone"`
		Type        string `json:"type"`
		AccessLevel string `json:"access_level"`
		Recursive   bool   `json:"recursive"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must be valid JSON")
		return
	}

	name := strings.TrimSpace(request.Name)
	if name == "" {
		writeValidationError(w, http.StatusBadRequest, "invalid_request", "ACL request validation failed", map[string]string{
			"name": "name is required",
		})
		return
	}

	principalType := strings.TrimSpace(request.Type)
	if principalType == "" {
		principalType = "user"
	}
	if principalType != "user" && principalType != "group" {
		writeValidationError(w, http.StatusBadRequest, "invalid_request", "ACL request validation failed", map[string]string{
			"type": "type must be user or group",
		})
		return
	}

	accessLevel := strings.TrimSpace(request.AccessLevel)
	if irodstypes.GetIRODSAccessLevelType(accessLevel) == irodstypes.IRODSAccessLevelNull {
		writeValidationError(w, http.StatusBadRequest, "invalid_request", "ACL request validation failed", map[string]string{
			"access_level": "access_level is required",
		})
		return
	}

	userType := irodstypes.IRODSUserRodsUser
	if principalType == "group" {
		userType = irodstypes.IRODSUserRodsGroup
	}

	created, err := h.paths.AddPathACL(r.Context(), objectPath, irodstypes.IRODSAccess{
		UserName:    name,
		UserZone:    strings.TrimSpace(request.Zone),
		UserType:    userType,
		AccessLevel: irodstypes.GetIRODSAccessLevelType(accessLevel),
	}, request.Recursive)
	if err != nil {
		lowerErr := strings.ToLower(err.Error())
		if strings.Contains(lowerErr, "name is required") || strings.Contains(lowerErr, "access_level is required") {
			writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		writePathError(w, err)
		return
	}

	created.Links = pathACLItemLinks(objectPath, created.ID)
	writeJSON(w, http.StatusCreated, map[string]any{
		"irods_path": objectPath,
		"acl":        created,
	})
}

func (h *Handler) putPathACL(w http.ResponseWriter, r *http.Request) {
	objectPath := queryIRODSPath(r)
	if objectPath == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "irods_path query parameter is required")
		return
	}

	aclID := pathValue(r, "acl_id")
	if aclID == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "acl_id path parameter is required")
		return
	}

	var request struct {
		AccessLevel string `json:"access_level"`
		Recursive   bool   `json:"recursive"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must be valid JSON")
		return
	}
	if irodstypes.GetIRODSAccessLevelType(strings.TrimSpace(request.AccessLevel)) == irodstypes.IRODSAccessLevelNull {
		writeValidationError(w, http.StatusBadRequest, "invalid_request", "ACL request validation failed", map[string]string{
			"access_level": "access_level is required",
		})
		return
	}

	updated, err := h.paths.UpdatePathACL(r.Context(), objectPath, aclID, request.AccessLevel, request.Recursive)
	if err != nil {
		lowerErr := strings.ToLower(err.Error())
		if strings.Contains(lowerErr, "access_level is required") || strings.Contains(lowerErr, "invalid acl id") {
			writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		writePathError(w, err)
		return
	}

	updated.Links = pathACLItemLinks(objectPath, updated.ID)
	writeJSON(w, http.StatusOK, map[string]any{
		"irods_path": objectPath,
		"acl":        updated,
	})
}

func (h *Handler) deletePathACL(w http.ResponseWriter, r *http.Request) {
	objectPath := queryIRODSPath(r)
	if objectPath == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "irods_path query parameter is required")
		return
	}

	aclID := pathValue(r, "acl_id")
	if aclID == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "acl_id path parameter is required")
		return
	}

	if err := h.paths.DeletePathACL(r.Context(), objectPath, aclID); err != nil {
		lowerErr := strings.ToLower(err.Error())
		if strings.Contains(lowerErr, "invalid acl id") {
			writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		writePathError(w, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) putPathACLInheritance(w http.ResponseWriter, r *http.Request) {
	objectPath := queryIRODSPath(r)
	if objectPath == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "irods_path query parameter is required")
		return
	}

	var request struct {
		Enabled   *bool `json:"enabled"`
		Recursive bool  `json:"recursive"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must be valid JSON")
		return
	}
	if request.Enabled == nil {
		writeValidationError(w, http.StatusBadRequest, "invalid_request", "ACL inheritance request validation failed", map[string]string{
			"enabled": "enabled is required",
		})
		return
	}

	if err := h.paths.SetPathACLInheritance(r.Context(), objectPath, *request.Enabled, request.Recursive); err != nil {
		writePathError(w, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) deletePathACLInheritance(w http.ResponseWriter, r *http.Request) {
	objectPath := queryIRODSPath(r)
	if objectPath == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "irods_path query parameter is required")
		return
	}

	recursive := false
	rawRecursive := strings.TrimSpace(r.URL.Query().Get("recursive"))
	if rawRecursive != "" {
		parsedRecursive, err := strconv.ParseBool(rawRecursive)
		if err != nil {
			writeValidationError(w, http.StatusBadRequest, "invalid_request", "ACL inheritance request validation failed", map[string]string{
				"recursive": "recursive must be a boolean",
			})
			return
		}
		recursive = parsedRecursive
	}

	if err := h.paths.SetPathACLInheritance(r.Context(), objectPath, false, recursive); err != nil {
		writePathError(w, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) postPath(w http.ResponseWriter, r *http.Request) {
	h.createPath(w, r)
}

func (h *Handler) postPathChildren(w http.ResponseWriter, r *http.Request) {
	h.createPath(w, r)
}

func (h *Handler) createPath(w http.ResponseWriter, r *http.Request) {
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

func (h *Handler) postPathContents(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Type"))), "multipart/form-data") {
		writeError(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "request must use multipart/form-data")
		return
	}

	if err := r.ParseMultipartForm(32 << 20); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must be valid multipart/form-data")
		return
	}

	parentPath := strings.TrimSpace(r.FormValue("parent_path"))
	if parentPath == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "parent_path is required")
		return
	}

	fileName := strings.TrimSpace(r.FormValue("file_name"))
	if fileName == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "file_name is required")
		return
	}

	checksum, err := multipartFormBool(r, "checksum")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	overwrite, err := multipartFormBool(r, "overwrite")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	content, _, err := r.FormFile("content")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "content is required")
		return
	}
	defer content.Close()

	uploaded, err := h.paths.UploadPathContents(r.Context(), parentPath, irods.PathContentsUploadOptions{
		FileName:  fileName,
		Content:   content,
		Checksum:  checksum,
		Overwrite: overwrite,
	})
	if err != nil {
		lowerErr := strings.ToLower(err.Error())
		if strings.Contains(lowerErr, "file_name is required") || strings.Contains(lowerErr, "content is required") {
			writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		writePathError(w, err)
		return
	}

	status := http.StatusCreated
	if uploaded.Action == "replaced" {
		status = http.StatusOK
	}

	writeJSON(w, status, pathContentsUploadResponse(uploaded))
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

func pathContentsUploadResponse(result domain.PathContentsUploadResult) domain.PathContentsUploadResult {
	result.Links = &domain.PathContentsUploadLinks{
		Path: &domain.ActionLink{
			Href:   "/api/v1/path?irods_path=" + url.QueryEscape(result.Path),
			Method: http.MethodGet,
		},
		Contents: &domain.ActionLink{
			Href:   "/api/v1/path/contents?irods_path=" + url.QueryEscape(result.Path),
			Method: http.MethodGet,
		},
		Parent: &domain.ActionLink{
			Href:   "/api/v1/path?irods_path=" + url.QueryEscape(result.ParentPath),
			Method: http.MethodGet,
		},
	}

	return result
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

func multipartFormBool(r *http.Request, fieldName string) (bool, error) {
	raw := strings.TrimSpace(r.FormValue(fieldName))
	if raw == "" {
		return false, nil
	}

	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("%s must be a boolean", fieldName)
	}

	return value, nil
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
	entry.CmdCue = pathCmdCueForEntry(entry)
	entry.Parent = buildParentLink(r, entry.Path)
	entry.PathSegments = buildPathSegments(entry.Path)
	return entry
}

func pathCmdCueForEntry(entry domain.PathEntry) *domain.CmdCue {
	var (
		cue cmdcues.Cue
		err error
	)

	switch strings.TrimSpace(entry.Kind) {
	case "collection":
		cue, err = cmdcues.BuildPutCue(entry.Path)
	case "data_object":
		cue, err = cmdcues.BuildGetCue(entry.Path)
	default:
		return nil
	}

	if err != nil {
		return nil
	}

	return &domain.CmdCue{
		Operation: string(cue.Operation),
		GoCmd:     cue.GoCmd,
		ICommand:  cue.ICommand,
	}
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

func pathACLResponse(_ *http.Request, acl domain.PathACL) domain.PathACL {
	acl.PathSegments = buildPathSegments(acl.IRODSPath)
	acl.Links = pathACLLinks(acl)
	acl.Users = pathACLEntryResponseList(acl.IRODSPath, acl.Users)
	acl.Groups = pathACLEntryResponseList(acl.IRODSPath, acl.Groups)
	return acl
}

func pathACLEntryResponseList(irodsPath string, entries []domain.PathACLEntry) []domain.PathACLEntry {
	if len(entries) == 0 {
		return []domain.PathACLEntry{}
	}

	mapped := make([]domain.PathACLEntry, 0, len(entries))
	for _, entry := range entries {
		entry.Links = pathACLItemLinks(irodsPath, entry.ID)
		mapped = append(mapped, entry)
	}
	return mapped
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
	aclPath := "/api/v1/path/acl?irods_path=" + url.QueryEscape(irodsPath)
	links := &domain.PathLinks{
		AVUs: &domain.ActionLink{
			Href:   avuPath,
			Method: http.MethodGet,
		},
		ACLs: &domain.ActionLink{
			Href:   aclPath,
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
		Resources: &domain.ActionLink{
			Href:   "/api/v1/resource",
			Method: http.MethodGet,
		},
	}

	if kind == "data_object" {
		replicasPath := "/api/v1/path/replicas?irods_path=" + url.QueryEscape(irodsPath)
		links.Replicas = &domain.ActionLink{
			Href:   replicasPath,
			Method: http.MethodGet,
		}
	}

	if kind == "collection" {
		createChildrenPath := "/api/v1/path?irods_path=" + url.QueryEscape(irodsPath)
		links.CreateChildCollection = &domain.ActionLink{
			Href:   createChildrenPath,
			Method: http.MethodPost,
		}
		links.CreateChildDataObject = &domain.ActionLink{
			Href:   createChildrenPath,
			Method: http.MethodPost,
		}
		inheritancePath := "/api/v1/path/acl/inheritance?irods_path=" + url.QueryEscape(irodsPath)
		links.SetInheritance = &domain.ActionLink{
			Href:   inheritancePath,
			Method: http.MethodPut,
		}
		links.DeleteInheritance = &domain.ActionLink{
			Href:   inheritancePath,
			Method: http.MethodDelete,
		}
	}

	return links
}

func pathACLLinks(acl domain.PathACL) *domain.PathACLLinks {
	irodsPath := strings.TrimSpace(acl.IRODSPath)
	if irodsPath == "" {
		return nil
	}

	aclPath := "/api/v1/path/acl?irods_path=" + url.QueryEscape(irodsPath)
	links := &domain.PathACLLinks{
		Path: &domain.ActionLink{
			Href:   "/api/v1/path?irods_path=" + url.QueryEscape(irodsPath),
			Method: http.MethodGet,
		},
		AddUser: &domain.ActionLink{
			Href:   aclPath,
			Method: http.MethodPost,
		},
	}

	if acl.Kind == "collection" && acl.InheritanceEnabled != nil {
		inheritancePath := "/api/v1/path/acl/inheritance?irods_path=" + url.QueryEscape(irodsPath)
		links.SetInheritance = &domain.ActionLink{
			Href:   inheritancePath,
			Method: http.MethodPut,
		}
	}

	return links
}

func pathACLItemLinks(irodsPath string, aclID string) *domain.PathACLItemLinks {
	irodsPath = strings.TrimSpace(irodsPath)
	aclID = strings.TrimSpace(aclID)
	if irodsPath == "" || aclID == "" {
		return nil
	}

	pathWithID := "/api/v1/path/acl/" + url.PathEscape(aclID) + "?irods_path=" + url.QueryEscape(irodsPath)
	return &domain.PathACLItemLinks{
		Update: &domain.ActionLink{
			Href:   pathWithID,
			Method: http.MethodPut,
		},
		Remove: &domain.ActionLink{
			Href:   pathWithID,
			Method: http.MethodDelete,
		},
	}
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
