package httpapi

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/michael-conway/irods-go-rest/internal/restservice"
)

func (h *Handler) getCurrentUserMembership(w http.ResponseWriter, r *http.Request) {
	limit, ok := optionalLimitQuery(w, r)
	if !ok {
		return
	}
	zone := h.userZoneFromRequest(r)

	membership, err := h.userAdmin.GetCurrentUserMembership(r.Context(), restservice.GroupsForUserOptions{
		Zone:  zone,
		Limit: limit,
	})
	if err != nil {
		writeUserError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"current_user": membership,
		"zone":         zone,
		"limit":        limit,
		"links": map[string]any{
			"self": map[string]string{
				"href":   usersAndGroupsHref("/api/v1/user/me", zone, "", "", nil, limit),
				"method": http.MethodGet,
			},
			"groups": map[string]string{
				"href":   userGroupsForUserHref(membership.User.Name, zone, limit),
				"method": http.MethodGet,
			},
		},
	})
}

func (h *Handler) getUserGroupSummaries(w http.ResponseWriter, r *http.Request) {
	zone := h.userZoneFromRequest(r)
	prefix, limit, ok := usersAndGroupsListOptionsFromRequest(w, r)
	if !ok {
		return
	}

	groups, err := h.userAdmin.ListGroupSummaries(r.Context(), restservice.UserGroupSummaryOptions{
		Zone:   zone,
		Prefix: prefix,
		Limit:  limit,
	})
	if err != nil {
		writeUserGroupError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"groups": groups,
		"count":  len(groups),
		"zone":   zone,
		"prefix": prefix,
		"limit":  limit,
		"links": map[string]any{
			"self": map[string]string{
				"href":   usersAndGroupsHref("/api/v1/usergroup/summary", zone, prefix, "", nil, limit),
				"method": http.MethodGet,
			},
		},
	})
}

func (h *Handler) getUserMembershipSummaries(w http.ResponseWriter, r *http.Request) {
	zone := h.userZoneFromRequest(r)
	prefix, limit, ok := usersAndGroupsListOptionsFromRequest(w, r)
	if !ok {
		return
	}
	userType := strings.TrimSpace(r.URL.Query().Get("type"))
	if userType != "" && !validRESTUserType(userType) {
		writeError(w, http.StatusBadRequest, "invalid_request", "type must be rodsuser or rodsadmin")
		return
	}

	users, err := h.userAdmin.ListUserMembershipSummaries(r.Context(), restservice.UserMembershipSummaryOptions{
		Zone:   zone,
		Prefix: prefix,
		Type:   userType,
		Limit:  limit,
	})
	if err != nil {
		writeUserError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"users":  users,
		"count":  len(users),
		"zone":   zone,
		"type":   userType,
		"prefix": prefix,
		"limit":  limit,
		"links": map[string]any{
			"self": map[string]string{
				"href":   usersAndGroupsHref("/api/v1/user/membership-summary", zone, prefix, userType, nil, limit),
				"method": http.MethodGet,
			},
		},
	})
}

func (h *Handler) getUserGroupsForUser(w http.ResponseWriter, r *http.Request) {
	username := pathValue(r, "user_name")
	if username == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "user_name path parameter is required")
		return
	}
	limit, ok := optionalLimitQuery(w, r)
	if !ok {
		return
	}
	zone := h.userZoneFromRequest(r)

	groups, err := h.userAdmin.ListGroupsForUser(r.Context(), restservice.GroupsForUserOptions{
		Zone:     zone,
		UserName: username,
		Limit:    limit,
	})
	if err != nil {
		writeUserGroupError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"groups":    groups,
		"count":     len(groups),
		"user_name": username,
		"zone":      zone,
		"limit":     limit,
	})
}

func (h *Handler) getPrincipals(w http.ResponseWriter, r *http.Request) {
	zone := h.userZoneFromRequest(r)
	query := strings.TrimSpace(r.URL.Query().Get("query"))
	if query == "" {
		query = strings.TrimSpace(r.URL.Query().Get("prefix"))
	}
	if query != "" && len(query) < 3 {
		writeError(w, http.StatusBadRequest, "invalid_request", "query must be at least 3 characters")
		return
	}
	if query == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "query is required")
		return
	}
	limit, ok := optionalLimitQuery(w, r)
	if !ok {
		return
	}
	kinds, ok := principalKindsFromRequest(w, r)
	if !ok {
		return
	}

	principals, err := h.userAdmin.SearchPrincipals(r.Context(), restservice.PrincipalSearchOptions{
		Zone:  zone,
		Query: query,
		Kinds: kinds,
		Limit: limit,
	})
	if err != nil {
		writeUserError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"principals": principals,
		"count":      len(principals),
		"zone":       zone,
		"query":      query,
		"kinds":      kinds,
		"limit":      limit,
		"links": map[string]any{
			"self": map[string]string{
				"href":   principalSearchHref(zone, query, kinds, limit),
				"method": http.MethodGet,
			},
		},
	})
}

func usersAndGroupsListOptionsFromRequest(w http.ResponseWriter, r *http.Request) (string, int, bool) {
	prefix := strings.TrimSpace(r.URL.Query().Get("prefix"))
	if prefix != "" && len(prefix) < 3 {
		writeError(w, http.StatusBadRequest, "invalid_request", "prefix must be at least 3 characters")
		return "", 0, false
	}
	limit, ok := optionalLimitQuery(w, r)
	return prefix, limit, ok
}

func optionalLimitQuery(w http.ResponseWriter, r *http.Request) (int, bool) {
	raw := strings.TrimSpace(r.URL.Query().Get("limit"))
	if raw == "" {
		return 0, true
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit < 1 || limit > 1000 {
		writeError(w, http.StatusBadRequest, "invalid_request", "limit query parameter must be an integer from 1 through 1000")
		return 0, false
	}
	return limit, true
}

func principalKindsFromRequest(w http.ResponseWriter, r *http.Request) ([]string, bool) {
	rawKinds := r.URL.Query()["kind"]
	if len(rawKinds) == 0 {
		rawKinds = r.URL.Query()["kinds"]
	}
	kinds := make([]string, 0, len(rawKinds))
	for _, raw := range rawKinds {
		for _, part := range strings.Split(raw, ",") {
			kind := strings.TrimSpace(part)
			if kind == "" {
				continue
			}
			if kind != "user" && kind != "group" {
				writeError(w, http.StatusBadRequest, "invalid_request", "kind must be user or group")
				return nil, false
			}
			kinds = append(kinds, kind)
		}
	}
	return kinds, true
}

func usersAndGroupsHref(base string, zone string, prefix string, userType string, kinds []string, limit int) string {
	query := url.Values{}
	if strings.TrimSpace(zone) != "" {
		query.Set("zone", strings.TrimSpace(zone))
	}
	if strings.TrimSpace(prefix) != "" {
		query.Set("prefix", strings.TrimSpace(prefix))
	}
	if strings.TrimSpace(userType) != "" {
		query.Set("type", strings.TrimSpace(userType))
	}
	for _, kind := range kinds {
		query.Add("kind", kind)
	}
	if limit > 0 {
		query.Set("limit", fmt.Sprintf("%d", limit))
	}
	if encoded := query.Encode(); encoded != "" {
		return base + "?" + encoded
	}
	return base
}

func principalSearchHref(zone string, queryValue string, kinds []string, limit int) string {
	query := url.Values{}
	if strings.TrimSpace(zone) != "" {
		query.Set("zone", strings.TrimSpace(zone))
	}
	query.Set("query", strings.TrimSpace(queryValue))
	for _, kind := range kinds {
		query.Add("kind", kind)
	}
	if limit > 0 {
		query.Set("limit", fmt.Sprintf("%d", limit))
	}
	return "/api/v1/principal?" + query.Encode()
}

func userGroupsForUserHref(username string, zone string, limit int) string {
	base := "/api/v1/user/" + url.PathEscape(strings.TrimSpace(username)) + "/usergroup"
	return usersAndGroupsHref(base, zone, "", "", nil, limit)
}
