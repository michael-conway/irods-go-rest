package httpapi

import (
	"encoding/base64"
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
	metadataext "github.com/michael-conway/go-irodsclient-extensions/metadata"
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
		writeErrorFromErr(w, r, http.StatusBadRequest, "invalid_request", err)
		return
	}

	object, err := h.paths.GetPath(r.Context(), objectPath, irods.PathLookupOptions{VerboseLevel: verboseLevel})
	if err != nil {
		if errors.Is(err, irods.ErrNotFound) {
			writeErrorFromErr(w, r, http.StatusNotFound, "not_found", err)
			return
		}
		if errors.Is(err, irods.ErrPermissionDenied) {
			writeErrorFromErr(w, r, http.StatusForbidden, "permission_denied", err)
			return
		}

		writeErrorFromErr(w, r, http.StatusInternalServerError, "internal_error", err)
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
		writeErrorFromErr(w, r, http.StatusBadRequest, "invalid_request", err)
		return
	}

	if err := h.paths.DeletePath(r.Context(), objectPath, force); err != nil {
		if errors.Is(err, irods.ErrConflict) {
			writeErrorFromErr(w, r, http.StatusConflict, "conflict", err)
			return
		}
		writePathError(w, r, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) patchPath(w http.ResponseWriter, r *http.Request) {
	objectPath := queryIRODSPath(r)
	if objectPath == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "irods_path query parameter is required")
		return
	}

	var request struct {
		Operation       string `json:"operation"`
		NewName         string `json:"new_name"`
		DestinationPath string `json:"destination_path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must be valid JSON")
		return
	}

	operation := strings.ToLower(strings.TrimSpace(request.Operation))
	if operation == "" {
		operation = string(irods.PathRelocateOperationMove)
	}
	if operation != string(irods.PathRelocateOperationMove) && operation != string(irods.PathRelocateOperationCopy) {
		writeError(w, http.StatusBadRequest, "invalid_request", "operation must be one of move or copy")
		return
	}

	if strings.TrimSpace(request.DestinationPath) == "" && strings.TrimSpace(request.NewName) == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "new_name or destination_path is required")
		return
	}

	relocated, err := h.paths.RelocatePath(r.Context(), objectPath, irods.PathRelocateOptions{
		Operation:       irods.PathRelocateOperation(operation),
		NewName:         request.NewName,
		DestinationPath: request.DestinationPath,
	})
	if err != nil {
		if errors.Is(err, irods.ErrInvalidRequest) {
			writeErrorFromErr(w, r, http.StatusBadRequest, "invalid_request", err)
			return
		}
		writePathError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, pathEntryResponse(r, relocated))
}

func (h *Handler) getPathChildren(w http.ResponseWriter, r *http.Request) {
	objectPath := queryIRODSPath(r)
	if objectPath == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "irods_path query parameter is required")
		return
	}

	searchOptions, err := queryPathChildrenSearchOptions(r)
	if err != nil {
		writeErrorFromErr(w, r, http.StatusBadRequest, "invalid_request", err)
		return
	}

	if strings.TrimSpace(searchOptions.NamePattern) == "" {
		children, listErr := h.paths.GetPathChildren(r.Context(), objectPath)
		if listErr != nil {
			if errors.Is(listErr, irods.ErrNotFound) {
				writeErrorFromErr(w, r, http.StatusNotFound, "not_found", listErr)
				return
			}
			if errors.Is(listErr, irods.ErrPermissionDenied) {
				writeErrorFromErr(w, r, http.StatusForbidden, "permission_denied", listErr)
				return
			}

			writeErrorFromErr(w, r, http.StatusInternalServerError, "internal_error", listErr)
			return
		}

		mappedChildren := make([]domain.PathEntry, 0, len(children))
		for _, child := range children {
			mappedChildren = append(mappedChildren, pathEntryResponse(r, child))
		}

		links := pathChildrenLinks(r, objectPath, len(mappedChildren), len(mappedChildren), searchOptions)

		writeJSON(w, http.StatusOK, map[string]any{
			"irods_path":    objectPath,
			"path_segments": buildPathSegments(objectPath),
			"links":         links,
			"children":      mappedChildren,
		})
		return
	}

	searchResult, err := h.paths.SearchPathChildren(r.Context(), objectPath, searchOptions)
	if err != nil {
		if errors.Is(err, irods.ErrNotFound) {
			writeErrorFromErr(w, r, http.StatusNotFound, "not_found", err)
			return
		}
		if errors.Is(err, irods.ErrPermissionDenied) {
			writeErrorFromErr(w, r, http.StatusForbidden, "permission_denied", err)
			return
		}

		writeErrorFromErr(w, r, http.StatusInternalServerError, "internal_error", err)
		return
	}

	mappedChildren := make([]domain.PathEntry, 0, len(searchResult.Children))
	for _, child := range searchResult.Children {
		mappedChildren = append(mappedChildren, pathEntryResponse(r, child))
	}

	recursive := searchResult.SearchScope == irods.PathChildrenSearchScopeSubtree || searchResult.SearchScope == irods.PathChildrenSearchScopeAbsolute
	links := pathChildrenLinks(r, objectPath, searchResult.MatchedCount, len(mappedChildren), searchOptions)

	writeJSON(w, http.StatusOK, map[string]any{
		"irods_path":    objectPath,
		"path_segments": buildPathSegments(objectPath),
		"links":         links,
		"children":      mappedChildren,
		"search": map[string]any{
			"name_pattern":   searchResult.NamePattern,
			"recursive":      recursive,
			"search_scope":   searchResult.SearchScope,
			"case_sensitive": searchResult.CaseSensitive,
			"matched_count":  searchResult.MatchedCount,
		},
	})
}

type pathQueryRequest struct {
	Version            string                          `json:"version,omitempty"`
	Type               string                          `json:"type,omitempty"`
	IRODSPath          string                          `json:"irods_path,omitempty"`
	SearchScope        metadataext.EntryQueryScopeMode `json:"search_scope,omitempty"`
	Scope              *metadataext.EntryQueryScope    `json:"scope,omitempty"`
	Kinds              []metadataext.EntryKind         `json:"kinds,omitempty"`
	Conditions         []metadataext.EntryCondition    `json:"conditions,omitempty"`
	Defaults           metadataext.EntryQueryDefaults  `json:"defaults,omitempty"`
	ReplicaPolicy      metadataext.ReplicaPolicy       `json:"replica_policy,omitempty"`
	Metadata           map[string]interface{}          `json:"metadata,omitempty"`
	Limit              int                             `json:"limit,omitempty"`
	PageToken          string                          `json:"page_token,omitempty"`
	IncludeTotals      *bool                           `json:"include_totals,omitempty"`
	IncludeMatchedAVUs *bool                           `json:"include_matched_avus,omitempty"`
}

type pathQueryPageResponse struct {
	Limit         int                      `json:"limit"`
	HasMore       bool                     `json:"has_more"`
	NextPageToken string                   `json:"next_page_token,omitempty"`
	Returned      pathQueryCountsResponse  `json:"returned"`
	Scanned       pathQueryCountsResponse  `json:"scanned"`
	Totals        *pathQueryCountsResponse `json:"totals,omitempty"`
}

type pathQueryCountsResponse struct {
	Collections int `json:"collections"`
	DataObjects int `json:"data_objects"`
}

type pathQueryResponse struct {
	IRODSPath    string                          `json:"irods_path,omitempty"`
	PathSegments []domain.PathSegmentLink        `json:"path_segments,omitempty"`
	Paths        []domain.PathEntry              `json:"paths"`
	MatchedAVUs  map[string][]domain.AVUMetadata `json:"matched_avus,omitempty"`
	Page         pathQueryPageResponse           `json:"page"`
	Query        pathQuerySummaryResponse        `json:"query"`
}

type pathQuerySummaryResponse struct {
	SearchScope        metadataext.EntryQueryScopeMode `json:"search_scope,omitempty"`
	Scope              *metadataext.EntryQueryScope    `json:"scope,omitempty"`
	Kinds              []metadataext.EntryKind         `json:"kinds"`
	Conditions         []metadataext.EntryCondition    `json:"conditions,omitempty"`
	IncludeTotals      bool                            `json:"include_totals,omitempty"`
	IncludeMatchedAVUs bool                            `json:"include_matched_avus,omitempty"`
}

func (h *Handler) postPathQuery(w http.ResponseWriter, r *http.Request) {
	request, err := decodePathQueryRequest(r)
	if err != nil {
		writeErrorFromErr(w, r, http.StatusBadRequest, "invalid_request", err)
		return
	}

	cursor, err := decodeEntryQueryPageToken(request.PageToken)
	if err != nil {
		writeErrorFromErr(w, r, http.StatusBadRequest, "invalid_request", err)
		return
	}

	definition, err := pathQueryDefinition(request)
	if err != nil {
		writeErrorFromErr(w, r, http.StatusBadRequest, "invalid_request", err)
		return
	}

	query, err := definition.ToEntryQuery(metadataext.EntryQueryExecutionOptions{
		Limit:              request.Limit,
		Cursor:             cursor,
		IncludeTotals:      request.IncludeTotals,
		IncludeMatchedAVUs: request.IncludeMatchedAVUs,
	})
	if err != nil {
		writeErrorFromErr(w, r, http.StatusBadRequest, "invalid_request", err)
		return
	}

	result, err := h.paths.QueryPathEntries(r.Context(), irods.PathQueryOptions{Query: query})
	if err != nil {
		if errors.Is(err, metadataext.ErrInvalidEntryQuery) {
			writeErrorFromErr(w, r, http.StatusBadRequest, "invalid_request", err)
			return
		}
		writePathError(w, r, err)
		return
	}

	paths := make([]domain.PathEntry, 0, len(result.Entries))
	for _, entry := range result.Entries {
		paths = append(paths, pathEntryResponse(r, entry))
	}

	response := pathQueryResponse{
		IRODSPath:    queryScopeRoot(result.Query),
		PathSegments: buildPathSegments(queryScopeRoot(result.Query)),
		Paths:        paths,
		MatchedAVUs:  matchedAVUResponseMap(r, result.MatchedAVUs),
		Page:         pathQueryPage(result.Page),
		Query:        pathQuerySummary(result.Query),
	}
	if response.IRODSPath == "" {
		response.PathSegments = nil
	}

	writeJSON(w, http.StatusOK, response)
}

func decodePathQueryRequest(r *http.Request) (pathQueryRequest, error) {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	var request pathQueryRequest
	if err := decoder.Decode(&request); err != nil {
		return pathQueryRequest{}, fmt.Errorf("request body must be valid path query JSON: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return pathQueryRequest{}, fmt.Errorf("request body must contain one JSON object")
	}
	return request, nil
}

func pathQueryDefinition(request pathQueryRequest) (metadataext.EntryQueryDefinition, error) {
	scope, err := pathQueryScope(request)
	if err != nil {
		return metadataext.EntryQueryDefinition{}, err
	}

	return metadataext.EntryQueryDefinition{
		Version:       request.Version,
		Type:          request.Type,
		Kinds:         request.Kinds,
		Scope:         scope,
		Conditions:    request.Conditions,
		Defaults:      request.Defaults,
		ReplicaPolicy: request.ReplicaPolicy,
		Metadata:      request.Metadata,
	}, nil
}

func pathQueryScope(request pathQueryRequest) (*metadataext.EntryQueryScope, error) {
	hasShortcutScope := strings.TrimSpace(request.IRODSPath) != "" || strings.TrimSpace(string(request.SearchScope)) != ""
	if request.Scope != nil && hasShortcutScope {
		return nil, fmt.Errorf("use either scope or irods_path/search_scope, not both")
	}
	if request.Scope != nil {
		scope := *request.Scope
		return &scope, nil
	}
	if !hasShortcutScope {
		return nil, nil
	}

	mode := request.SearchScope
	if mode == "" {
		mode = metadataext.EntryQueryScopeChildren
	}
	switch mode {
	case metadataext.EntryQueryScopeSelf, metadataext.EntryQueryScopeChildren, metadataext.EntryQueryScopeDescendants, metadataext.EntryQueryScopeAbsolute:
	default:
		return nil, fmt.Errorf("search_scope must be one of self, children, descendants, or absolute")
	}

	return &metadataext.EntryQueryScope{
		Root: strings.TrimSpace(request.IRODSPath),
		Mode: mode,
	}, nil
}

func decodeEntryQueryPageToken(token string) (*metadataext.EntryQueryCursor, error) {
	token = strings.TrimSpace(token)
	if token == "" {
		return nil, nil
	}

	data, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, fmt.Errorf("page_token is invalid")
	}

	var cursor metadataext.EntryQueryCursor
	if err := json.Unmarshal(data, &cursor); err != nil {
		return nil, fmt.Errorf("page_token is invalid")
	}
	return &cursor, nil
}

func encodeEntryQueryPageToken(cursor *metadataext.EntryQueryCursor) string {
	if cursor == nil {
		return ""
	}

	data, err := json.Marshal(cursor)
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(data)
}

func pathQueryPage(page metadataext.EntryQueryPage) pathQueryPageResponse {
	response := pathQueryPageResponse{
		Limit:         page.Limit,
		HasMore:       page.HasMore,
		NextPageToken: encodeEntryQueryPageToken(page.Next),
		Returned:      pathQueryCounts(page.Returned),
		Scanned:       pathQueryCounts(page.Scanned),
	}
	if page.Totals != nil {
		totals := pathQueryCounts(*page.Totals)
		response.Totals = &totals
	}
	return response
}

func pathQueryCounts(counts metadataext.EntryQueryCounts) pathQueryCountsResponse {
	return pathQueryCountsResponse{
		Collections: counts.Collections,
		DataObjects: counts.DataObjects,
	}
}

func pathQuerySummary(query metadataext.EntryQuery) pathQuerySummaryResponse {
	summary := pathQuerySummaryResponse{
		Kinds:              append([]metadataext.EntryKind(nil), query.Kinds...),
		Conditions:         append([]metadataext.EntryCondition(nil), query.Conditions...),
		IncludeTotals:      query.IncludeTotals,
		IncludeMatchedAVUs: query.IncludeMatchedAVUs,
	}
	if query.Scope != nil {
		summary.SearchScope = query.Scope.Mode
		scope := *query.Scope
		scope.Root = strings.TrimSpace(scope.Root)
		summary.Scope = &scope
	}
	return summary
}

func queryScopeRoot(query metadataext.EntryQuery) string {
	if query.Scope == nil || query.Scope.Mode == metadataext.EntryQueryScopeAbsolute {
		return ""
	}
	return strings.TrimSpace(query.Scope.Root)
}

func (h *Handler) getPathReplicas(w http.ResponseWriter, r *http.Request) {
	objectPath := queryIRODSPath(r)
	if objectPath == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "irods_path query parameter is required")
		return
	}

	verboseLevel, err := queryVerboseLevel(r)
	if err != nil {
		writeErrorFromErr(w, r, http.StatusBadRequest, "invalid_request", err)
		return
	}

	replicas, err := h.paths.GetPathReplicas(r.Context(), objectPath, verboseLevel)
	if err != nil {
		writePathError(w, r, err)
		return
	}
	replicas = pathReplicaResponseList(objectPath, replicas)

	writeJSON(w, http.StatusOK, map[string]any{
		"irods_path":    objectPath,
		"path_segments": buildPathSegments(objectPath),
		"links":         pathReplicasLinks(r, objectPath),
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
		if errors.Is(err, irods.ErrInvalidRequest) {
			writeErrorFromErr(w, r, http.StatusBadRequest, "invalid_request", err)
			return
		}
		writePathError(w, r, err)
		return
	}
	replicas = pathReplicaResponseList(objectPath, replicas)

	writeJSON(w, http.StatusCreated, map[string]any{
		"irods_path":    objectPath,
		"path_segments": buildPathSegments(objectPath),
		"links":         pathReplicasLinks(r, objectPath),
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
		if errors.Is(err, irods.ErrInvalidRequest) {
			writeErrorFromErr(w, r, http.StatusBadRequest, "invalid_request", err)
			return
		}
		writePathError(w, r, err)
		return
	}
	replicas = pathReplicaResponseList(objectPath, replicas)

	writeJSON(w, http.StatusOK, map[string]any{
		"irods_path":    objectPath,
		"path_segments": buildPathSegments(objectPath),
		"links":         pathReplicasLinks(r, objectPath),
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
		if errors.Is(err, irods.ErrInvalidRequest) {
			writeErrorFromErr(w, r, http.StatusBadRequest, "invalid_request", err)
			return
		}
		writePathError(w, r, err)
		return
	}
	replicas = pathReplicaResponseList(objectPath, replicas)

	writeJSON(w, http.StatusOK, map[string]any{
		"irods_path":    objectPath,
		"path_segments": buildPathSegments(objectPath),
		"links":         pathReplicasLinks(r, objectPath),
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
		writePathError(w, r, err)
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
		if errors.Is(err, irods.ErrInvalidRequest) {
			writeErrorFromErr(w, r, http.StatusBadRequest, "invalid_request", err)
			return
		}
		writePathError(w, r, err)
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
		if errors.Is(err, irods.ErrInvalidRequest) {
			writeErrorFromErr(w, r, http.StatusBadRequest, "invalid_request", err)
			return
		}
		writePathError(w, r, err)
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
		if errors.Is(err, irods.ErrInvalidRequest) {
			writeErrorFromErr(w, r, http.StatusBadRequest, "invalid_request", err)
			return
		}
		writePathError(w, r, err)
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
		writePathError(w, r, err)
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
		writePathError(w, r, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) postPath(w http.ResponseWriter, r *http.Request) {
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
		if errors.Is(err, irods.ErrInvalidRequest) {
			writeErrorFromErr(w, r, http.StatusBadRequest, "invalid_request", err)
			return
		}
		writePathError(w, r, err)
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
		writeErrorFromErr(w, r, http.StatusBadRequest, "invalid_request", err)
		return
	}

	overwrite, err := multipartFormBool(r, "overwrite")
	if err != nil {
		writeErrorFromErr(w, r, http.StatusBadRequest, "invalid_request", err)
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
		if errors.Is(err, irods.ErrInvalidRequest) {
			writeErrorFromErr(w, r, http.StatusBadRequest, "invalid_request", err)
			return
		}
		writePathError(w, r, err)
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
		writeErrorFromErr(w, r, http.StatusBadRequest, "invalid_request", err)
		return
	}

	metadata, err := h.paths.GetPathMetadata(r.Context(), objectPath)
	if err != nil {
		if errors.Is(err, irods.ErrNotFound) {
			writeErrorFromErr(w, r, http.StatusNotFound, "not_found", err)
			return
		}
		if errors.Is(err, irods.ErrPermissionDenied) {
			writeErrorFromErr(w, r, http.StatusForbidden, "permission_denied", err)
			return
		}

		writeErrorFromErr(w, r, http.StatusInternalServerError, "internal_error", err)
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
		if errors.Is(err, irods.ErrInvalidRequest) {
			writeErrorFromErr(w, r, http.StatusBadRequest, "invalid_request", err)
			return
		}
		writePathError(w, r, err)
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
		if errors.Is(err, irods.ErrInvalidRequest) {
			writeErrorFromErr(w, r, http.StatusBadRequest, "invalid_request", err)
			return
		}
		writePathError(w, r, err)
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
		if errors.Is(err, irods.ErrInvalidRequest) {
			writeErrorFromErr(w, r, http.StatusBadRequest, "invalid_request", err)
			return
		}
		writePathError(w, r, err)
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
		writePathError(w, r, err)
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
		writePathError(w, r, err)
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
			writeErrorFromErr(w, r, http.StatusNotFound, "not_found", err)
			return
		}
		if errors.Is(err, irods.ErrPermissionDenied) {
			writeErrorFromErr(w, r, http.StatusForbidden, "permission_denied", err)
			return
		}

		writeErrorFromErr(w, r, http.StatusInternalServerError, "internal_error", err)
		return
	}
	defer func() {
		if content.Reader != nil {
			_ = content.Reader.Close()
		}
	}()

	rangeHeader := ""
	// RFC 7233 §3.1: Range is only defined for GET; ignore for HEAD.
	if !headOnly {
		rangeHeader = r.Header.Get("Range")
	}

	status, contentRange, start, end, err := resolveByteRange(rangeHeader, content)
	if err != nil {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", content.Size))
		writeErrorFromErr(w, r, http.StatusRequestedRangeNotSatisfiable, "invalid_range", err)
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
		writeErrorFromErr(w, r, http.StatusInternalServerError, "internal_error", err)
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

func writePathError(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, irods.ErrNotFound) {
		writeErrorFromErr(w, r, http.StatusNotFound, "not_found", err)
		return
	}
	if errors.Is(err, irods.ErrPermissionDenied) {
		writeErrorFromErr(w, r, http.StatusForbidden, "permission_denied", err)
		return
	}
	if errors.Is(err, irods.ErrConflict) {
		writeErrorFromErr(w, r, http.StatusConflict, "conflict", err)
		return
	}

	writeErrorFromErr(w, r, http.StatusInternalServerError, "internal_error", err)
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

func queryPathChildrenSearchOptions(r *http.Request) (irods.PathChildrenListOptions, error) {
	query := r.URL.Query()
	options := irods.PathChildrenListOptions{
		NamePattern: strings.TrimSpace(query.Get("name_pattern")),
		SearchScope: irods.PathChildrenSearchScopeChildren,
		Sort:        strings.TrimSpace(query.Get("sort")),
		Order:       strings.TrimSpace(query.Get("order")),
	}

	caseSensitiveRaw := strings.TrimSpace(query.Get("case_sensitive"))
	if caseSensitiveRaw == "" {
		options.CaseSensitive = true
	} else {
		caseSensitive, err := strconv.ParseBool(caseSensitiveRaw)
		if err != nil {
			return irods.PathChildrenListOptions{}, fmt.Errorf("case_sensitive query parameter must be a boolean")
		}
		options.CaseSensitive = caseSensitive
	}

	searchScope := strings.TrimSpace(query.Get("search_scope"))
	switch strings.ToLower(searchScope) {
	case "", string(irods.PathChildrenSearchScopeChildren):
		options.SearchScope = irods.PathChildrenSearchScopeChildren
	case string(irods.PathChildrenSearchScopeSubtree):
		options.SearchScope = irods.PathChildrenSearchScopeSubtree
	case string(irods.PathChildrenSearchScopeAbsolute):
		options.SearchScope = irods.PathChildrenSearchScopeAbsolute
	default:
		return irods.PathChildrenListOptions{}, fmt.Errorf("search_scope query parameter must be one of children, subtree, or absolute")
	}

	if rawRecursive := strings.TrimSpace(query.Get("recursive")); rawRecursive != "" {
		return irods.PathChildrenListOptions{}, fmt.Errorf("recursive query parameter is no longer supported; use search_scope=children or search_scope=subtree")
	}

	if rawLimit := strings.TrimSpace(query.Get("limit")); rawLimit != "" {
		limit, err := strconv.Atoi(rawLimit)
		if err != nil || limit < 1 || limit > 1000 {
			return irods.PathChildrenListOptions{}, fmt.Errorf("limit query parameter must be an integer from 1 through 1000")
		}
		options.Limit = limit
	}

	if rawOffset := strings.TrimSpace(query.Get("offset")); rawOffset != "" {
		offset, err := strconv.Atoi(rawOffset)
		if err != nil || offset < 0 {
			return irods.PathChildrenListOptions{}, fmt.Errorf("offset query parameter must be a non-negative integer")
		}
		options.Offset = offset
	}

	if options.Sort != "" {
		switch strings.ToLower(options.Sort) {
		case "path", "name", "kind", "size", "created_at", "updated_at":
		default:
			return irods.PathChildrenListOptions{}, fmt.Errorf("sort query parameter must be one of path, name, kind, size, created_at, or updated_at")
		}
	}

	if options.Order != "" {
		switch strings.ToLower(options.Order) {
		case "asc", "desc":
		default:
			return irods.PathChildrenListOptions{}, fmt.Errorf("order query parameter must be asc or desc")
		}
	}

	return options, nil
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
	entry.CmdCues = pathCmdCuesForEntry(entry)
	entry.PathSegments = buildPathSegments(entry.Path)
	return entry
}

func pathCmdCuesForEntry(entry domain.PathEntry) []domain.CmdCue {
	switch strings.TrimSpace(entry.Kind) {
	case "collection":
		return buildCollectionCmdCues(entry.Path)
	case "data_object":
		return buildDataObjectCmdCues(entry.Path)
	default:
		return nil
	}
}

func buildCollectionCmdCues(collectionIRODSPath string) []domain.CmdCue {
	collectionIRODSPath = strings.TrimSpace(collectionIRODSPath)
	if collectionIRODSPath == "" {
		return nil
	}

	return []domain.CmdCue{
		{
			Operation: "put",
			GoCmd:     fmt.Sprintf("gocmd put -r %s %s", "<LOCAL_PATH>", quoteCuePath(collectionIRODSPath)),
			ICommand:  fmt.Sprintf("iput -r %s %s", "<LOCAL_PATH>", quoteCuePath(collectionIRODSPath)),
		},
		{
			Operation: "get",
			GoCmd:     fmt.Sprintf("gocmd get -r %s %s", quoteCuePath(collectionIRODSPath), "<DESTINATION_PATH>"),
			ICommand:  fmt.Sprintf("iget -r %s %s", quoteCuePath(collectionIRODSPath), "<DESTINATION_PATH>"),
		},
		{
			Operation: "phymove",
			ICommand:  fmt.Sprintf("iphymv -r -S %s -R %s %s", "<srcResource>", "<targetResource>", quoteCuePath(collectionIRODSPath)),
		},
		{
			Operation: "replicate",
			ICommand:  fmt.Sprintf("irepl -r -S %s -R %s %s", "<srcResource>", "<targetResource>", quoteCuePath(collectionIRODSPath)),
		},
	}
}

func buildDataObjectCmdCues(objectIRODSPath string) []domain.CmdCue {
	objectIRODSPath = strings.TrimSpace(objectIRODSPath)
	if objectIRODSPath == "" {
		return nil
	}

	parentPath := path.Dir(path.Clean(objectIRODSPath))
	putCue, err := cmdcues.BuildPutCue(parentPath)
	if err != nil {
		return nil
	}
	getCue, err := cmdcues.BuildGetCue(objectIRODSPath)
	if err != nil {
		return nil
	}

	return []domain.CmdCue{
		{
			Operation: string(putCue.Operation),
			GoCmd:     putCue.GoCmd,
			ICommand:  putCue.ICommand,
		},
		{
			Operation: string(getCue.Operation),
			GoCmd:     getCue.GoCmd,
			ICommand:  getCue.ICommand,
		},
		{
			Operation: "phymove",
			ICommand:  fmt.Sprintf("iphymv -S %s -R %s %s", "<srcResource>", "<targetResource>", quoteCuePath(objectIRODSPath)),
		},
		{
			Operation: "replicate",
			ICommand:  fmt.Sprintf("irepl -S %s -R %s %s", "<srcResource>", "<targetResource>", quoteCuePath(objectIRODSPath)),
		},
	}
}

func quoteCuePath(pathValue string) string {
	pathValue = strings.TrimSpace(pathValue)
	if pathValue == "" {
		return "''"
	}
	escaped := strings.ReplaceAll(pathValue, `'`, `'"'"'`)
	return "'" + escaped + "'"
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

func matchedAVUResponseMap(r *http.Request, matched map[string][]domain.AVUMetadata) map[string][]domain.AVUMetadata {
	if len(matched) == 0 {
		return nil
	}

	result := make(map[string][]domain.AVUMetadata, len(matched))
	for irodsPath, avus := range matched {
		mapped := avuMetadataResponseList(r, irodsPath, avus)
		if len(mapped) > 0 {
			result[irodsPath] = mapped
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
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

func pathLinksForEntry(irodsPath string, kind string) *domain.PathLinks {
	irodsPath = strings.TrimSpace(irodsPath)
	if irodsPath == "" {
		return nil
	}

	pathLink := "/api/v1/path?irods_path=" + url.QueryEscape(irodsPath)
	avuPath := "/api/v1/path/avu?irods_path=" + url.QueryEscape(irodsPath)
	aclPath := "/api/v1/path/acl?irods_path=" + url.QueryEscape(irodsPath)
	replicaPath := "/api/v1/path/replicas?irods_path=" + url.QueryEscape(irodsPath)
	parentPath := path.Dir(path.Clean(irodsPath))
	uploadPath := "/api/v1/path/contents?parent_path=" + url.QueryEscape(irodsPath)
	dataObjectParentPath := "/"
	if parentPath != "." && parentPath != "" {
		dataObjectParentPath = parentPath
	}

	links := &domain.PathLinks{
		Self: &domain.ActionLink{
			Href:   pathLink,
			Method: http.MethodGet,
		},
		Details: &domain.ActionLink{
			Href:   pathLink,
			Method: http.MethodGet,
		},
		Update: &domain.ActionLink{
			Href:   pathLink,
			Method: http.MethodPatch,
		},
		Delete: &domain.ActionLink{
			Href:   pathLink,
			Method: http.MethodDelete,
		},
		Relocate: &domain.ActionLink{
			Href:   pathLink,
			Method: http.MethodPatch,
		},
		Move: &domain.ActionLink{
			Href:   pathLink,
			Method: http.MethodPatch,
		},
		Copy: &domain.ActionLink{
			Href:   pathLink,
			Method: http.MethodPatch,
		},
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
	if parentPath != "." && parentPath != "" && parentPath != path.Clean(irodsPath) {
		links.Parent = &domain.ActionLink{
			Href:   "/api/v1/path?irods_path=" + url.QueryEscape(parentPath),
			Method: http.MethodGet,
		}
	}

	if kind == "data_object" {
		links.Replicas = &domain.ActionLink{
			Href:   replicaPath,
			Method: http.MethodGet,
		}
		links.DownloadContents = &domain.ActionLink{
			Href:   "/api/v1/path/contents?irods_path=" + url.QueryEscape(irodsPath),
			Method: http.MethodGet,
		}
		links.ReplaceContents = &domain.ActionLink{
			Href:   "/api/v1/path/contents?parent_path=" + url.QueryEscape(dataObjectParentPath) + "&file_name=" + url.QueryEscape(path.Base(path.Clean(irodsPath))),
			Method: http.MethodPost,
		}
		links.AddReplica = &domain.ActionLink{
			Href:   replicaPath,
			Method: http.MethodPost,
		}
		links.MoveReplica = &domain.ActionLink{
			Href:   replicaPath,
			Method: http.MethodPatch,
		}
		links.TrimReplica = &domain.ActionLink{
			Href:   replicaPath,
			Method: http.MethodDelete,
		}
	}

	if kind == "collection" {
		createChildrenPath := "/api/v1/path?irods_path=" + url.QueryEscape(irodsPath)
		links.Children = &domain.ActionLink{
			Href:   "/api/v1/path/children?irods_path=" + url.QueryEscape(irodsPath),
			Method: http.MethodGet,
		}
		links.CreateChildCollection = &domain.ActionLink{
			Href:   createChildrenPath,
			Method: http.MethodPost,
		}
		links.CreateChildDataObject = &domain.ActionLink{
			Href:   createChildrenPath,
			Method: http.MethodPost,
		}
		links.UploadContents = &domain.ActionLink{
			Href:   uploadPath,
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

func actionLinkFromRequest(r *http.Request) *domain.ActionLink {
	if r == nil || r.URL == nil {
		return nil
	}

	href := r.URL.Path
	if strings.TrimSpace(r.URL.RawQuery) != "" {
		href += "?" + r.URL.RawQuery
	}

	return &domain.ActionLink{
		Href:   href,
		Method: http.MethodGet,
	}
}

func pathChildrenLinks(r *http.Request, collectionPath string, total int, count int, options irods.PathChildrenListOptions) *domain.PathChildrenLinks {
	collectionPath = strings.TrimSpace(collectionPath)
	if collectionPath == "" {
		return nil
	}

	links := &domain.PathChildrenLinks{
		Self: actionLinkFromRequest(r),
		CreateChildCollection: &domain.ActionLink{
			Href:   "/api/v1/path?irods_path=" + url.QueryEscape(collectionPath),
			Method: http.MethodPost,
		},
		CreateChildDataObject: &domain.ActionLink{
			Href:   "/api/v1/path?irods_path=" + url.QueryEscape(collectionPath),
			Method: http.MethodPost,
		},
		UploadContents: &domain.ActionLink{
			Href:   "/api/v1/path/contents?parent_path=" + url.QueryEscape(collectionPath),
			Method: http.MethodPost,
		},
	}

	parentPath := path.Dir(path.Clean(collectionPath))
	if parentPath != "." && parentPath != "" && parentPath != path.Clean(collectionPath) {
		links.Parent = &domain.ActionLink{
			Href:   "/api/v1/path?irods_path=" + url.QueryEscape(parentPath),
			Method: http.MethodGet,
		}
	}

	if r == nil || r.URL == nil || options.Limit <= 0 {
		return links
	}

	if options.Offset > 0 {
		prevOffset := options.Offset - options.Limit
		if prevOffset < 0 {
			prevOffset = 0
		}
		prevQuery := cloneURLValues(r.URL.Query())
		prevQuery.Set("offset", strconv.Itoa(prevOffset))
		prevQuery.Set("limit", strconv.Itoa(options.Limit))
		links.Prev = &domain.ActionLink{
			Href:   r.URL.Path + "?" + prevQuery.Encode(),
			Method: http.MethodGet,
		}
	}

	if options.Offset+count < total {
		nextOffset := options.Offset + count
		nextQuery := cloneURLValues(r.URL.Query())
		nextQuery.Set("offset", strconv.Itoa(nextOffset))
		nextQuery.Set("limit", strconv.Itoa(options.Limit))
		links.Next = &domain.ActionLink{
			Href:   r.URL.Path + "?" + nextQuery.Encode(),
			Method: http.MethodGet,
		}
	}

	return links
}

func cloneURLValues(values url.Values) url.Values {
	cloned := url.Values{}
	for key, entries := range values {
		copied := make([]string, len(entries))
		copy(copied, entries)
		cloned[key] = copied
	}
	return cloned
}

func pathReplicasLinks(r *http.Request, irodsPath string) *domain.PathReplicasLinks {
	irodsPath = strings.TrimSpace(irodsPath)
	if irodsPath == "" {
		return nil
	}

	replicaPath := "/api/v1/path/replicas?irods_path=" + url.QueryEscape(irodsPath)
	links := &domain.PathReplicasLinks{
		Self: actionLinkFromRequest(r),
		AddReplica: &domain.ActionLink{
			Href:   replicaPath,
			Method: http.MethodPost,
		},
		MoveReplica: &domain.ActionLink{
			Href:   replicaPath,
			Method: http.MethodPatch,
		},
		TrimReplica: &domain.ActionLink{
			Href:   replicaPath,
			Method: http.MethodDelete,
		},
	}

	if links.Self == nil {
		links.Self = &domain.ActionLink{
			Href:   replicaPath,
			Method: http.MethodGet,
		}
	}

	return links
}

func pathReplicaResponseList(irodsPath string, replicas []domain.PathReplica) []domain.PathReplica {
	if len(replicas) == 0 {
		return nil
	}

	mapped := make([]domain.PathReplica, 0, len(replicas))
	for _, replica := range replicas {
		replica.Links = pathReplicaLinks(irodsPath, replica)
		mapped = append(mapped, replica)
	}
	return mapped
}

func pathReplicaLinks(irodsPath string, replica domain.PathReplica) *domain.PathReplicaLinks {
	irodsPath = strings.TrimSpace(irodsPath)
	if irodsPath == "" {
		return nil
	}

	replicaPath := "/api/v1/path/replicas?irods_path=" + url.QueryEscape(irodsPath)
	links := &domain.PathReplicaLinks{
		Trim: &domain.ActionLink{
			Href:   replicaPath,
			Method: http.MethodDelete,
		},
	}

	resourceName := strings.TrimSpace(replica.ResourceName)
	if resourceName != "" {
		links.ResourceDetails = &domain.ActionLink{
			Href:   "/api/v1/resource/" + url.PathEscape(resourceName),
			Method: http.MethodGet,
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

	trimmedHeader := strings.TrimSpace(rangeHeader)
	if !strings.HasPrefix(strings.ToLower(trimmedHeader), "bytes=") {
		// RFC 7233 §3.1: unknown range units must be ignored.
		return http.StatusOK, "", 0, size, nil
	}

	spec := strings.TrimSpace(trimmedHeader[len("bytes="):])
	if spec == "" || strings.Contains(spec, ",") {
		return 0, "", 0, 0, fmt.Errorf("only a single byte range is supported")
	}

	parts := strings.SplitN(spec, "-", 2)
	if len(parts) != 2 {
		return 0, "", 0, 0, fmt.Errorf("invalid byte range")
	}

	startRaw := strings.TrimSpace(parts[0])
	endRaw := strings.TrimSpace(parts[1])
	if startRaw == "" {
		// Suffix-byte-range-spec: bytes=-N (last N bytes)
		suffixLength, err := strconv.ParseInt(endRaw, 10, 64)
		if err != nil || suffixLength <= 0 || size == 0 {
			return 0, "", 0, 0, fmt.Errorf("invalid byte range end")
		}

		start := int64(0)
		if suffixLength < size {
			start = size - suffixLength
		}
		endExclusive := size
		return http.StatusPartialContent, fmt.Sprintf("bytes %d-%d/%d", start, endExclusive-1, size), start, endExclusive, nil
	}

	start, err := strconv.ParseInt(startRaw, 10, 64)
	if err != nil || start < 0 || start >= size {
		return 0, "", 0, 0, fmt.Errorf("invalid byte range start")
	}

	endExclusive := size
	if endRaw != "" {
		endInclusive, err := strconv.ParseInt(endRaw, 10, 64)
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
