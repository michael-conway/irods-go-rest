package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/michael-conway/irods-go-rest/internal/domain"
	"github.com/michael-conway/irods-go-rest/internal/irods"
	"github.com/michael-conway/irods-go-rest/internal/restservice"
)

type userGroupCreateRequest struct {
	Name string `json:"name"`
}

type userGroupMemberRequest struct {
	UserName string `json:"user_name"`
}

func (h *Handler) getUserGroups(w http.ResponseWriter, r *http.Request) {
	zone := h.userZoneFromRequest(r)
	prefix := strings.TrimSpace(r.URL.Query().Get("prefix"))
	if prefix != "" && len(prefix) < 3 {
		writeError(w, http.StatusBadRequest, "invalid_request", "prefix must be at least 3 characters")
		return
	}

	groups, err := h.userGroups.ListUserGroups(r.Context(), restservice.UserGroupListOptions{
		Zone:   zone,
		Prefix: prefix,
	})
	if err != nil {
		writeUserGroupError(w, err)
		return
	}

	groups = userGroupResponseList(groups)
	payload := map[string]any{
		"groups": groups,
		"count":  len(groups),
		"zone":   zone,
		"links": map[string]domain.ActionLink{
			"self": {
				Href:   userGroupListHref(zone, prefix),
				Method: http.MethodGet,
			},
			"create": {
				Href:   userGroupListHref(zone, ""),
				Method: http.MethodPost,
			},
		},
	}
	if prefix != "" {
		payload["prefix"] = prefix
	}

	writeJSON(w, http.StatusOK, payload)
}

func (h *Handler) getUserGroup(w http.ResponseWriter, r *http.Request) {
	groupName := pathValue(r, "group_name")
	if groupName == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "group_name path parameter is required")
		return
	}

	group, err := h.userGroups.GetUserGroup(r.Context(), groupName, h.userZoneFromRequest(r))
	if err != nil {
		writeUserGroupError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"group": userGroupResponse(group),
	})
}

func (h *Handler) postUserGroup(w http.ResponseWriter, r *http.Request) {
	var request userGroupCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must be valid JSON")
		return
	}

	groupName := strings.TrimSpace(request.Name)
	if groupName == "" {
		writeValidationError(w, http.StatusBadRequest, "invalid_request", "user group create validation failed", map[string]string{
			"name": "name is required",
		})
		return
	}

	group, err := h.userGroups.CreateUserGroup(r.Context(), groupName, h.userZoneFromRequest(r))
	if err != nil {
		writeUserGroupError(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"group": userGroupResponse(group),
	})
}

func (h *Handler) deleteUserGroup(w http.ResponseWriter, r *http.Request) {
	groupName := pathValue(r, "group_name")
	if groupName == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "group_name path parameter is required")
		return
	}

	if err := h.userGroups.DeleteUserGroup(r.Context(), groupName, h.userZoneFromRequest(r)); err != nil {
		writeUserGroupError(w, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) postUserGroupMember(w http.ResponseWriter, r *http.Request) {
	groupName := pathValue(r, "group_name")
	if groupName == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "group_name path parameter is required")
		return
	}

	var request userGroupMemberRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must be valid JSON")
		return
	}

	username := strings.TrimSpace(request.UserName)
	if username == "" {
		writeValidationError(w, http.StatusBadRequest, "invalid_request", "user group member validation failed", map[string]string{
			"user_name": "user_name is required",
		})
		return
	}

	group, err := h.userGroups.AddUserToGroup(r.Context(), groupName, username, h.userZoneFromRequest(r))
	if err != nil {
		writeUserGroupError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"group": userGroupResponse(group),
	})
}

func (h *Handler) deleteUserGroupMember(w http.ResponseWriter, r *http.Request) {
	groupName := pathValue(r, "group_name")
	if groupName == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "group_name path parameter is required")
		return
	}

	username := pathValue(r, "user_name")
	if username == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "user_name path parameter is required")
		return
	}

	group, err := h.userGroups.RemoveUserFromGroup(r.Context(), groupName, username, h.userZoneFromRequest(r))
	if err != nil {
		writeUserGroupError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"group": userGroupResponse(group),
	})
}

func userGroupResponseList(groups []domain.UserGroup) []domain.UserGroup {
	if len(groups) == 0 {
		return []domain.UserGroup{}
	}

	mapped := make([]domain.UserGroup, 0, len(groups))
	for _, group := range groups {
		mapped = append(mapped, userGroupResponse(group))
	}
	return mapped
}

func userGroupResponse(group domain.UserGroup) domain.UserGroup {
	group.Links = userGroupLinks(group)
	group.Members = userGroupMemberResponseList(group.Name, group.Zone, group.Members)
	return group
}

func userGroupMemberResponseList(groupName string, zone string, members []domain.UserGroupMember) []domain.UserGroupMember {
	if len(members) == 0 {
		return []domain.UserGroupMember{}
	}

	mapped := make([]domain.UserGroupMember, 0, len(members))
	for _, member := range members {
		member.Links = userGroupMemberLinks(groupName, zone, member.Name, member.Zone)
		mapped = append(mapped, member)
	}
	return mapped
}

func userGroupLinks(group domain.UserGroup) *domain.UserGroupLinks {
	groupName := strings.TrimSpace(group.Name)
	if groupName == "" {
		return nil
	}

	href := userGroupHref(groupName, group.Zone)
	return &domain.UserGroupLinks{
		Self: &domain.ActionLink{
			Href:   href,
			Method: http.MethodGet,
		},
		Delete: &domain.ActionLink{
			Href:   href,
			Method: http.MethodDelete,
		},
		AddMember: &domain.ActionLink{
			Href:   userGroupMemberBaseHref(groupName, group.Zone),
			Method: http.MethodPost,
		},
	}
}

func userGroupMemberLinks(groupName string, groupZone string, username string, userZone string) *domain.UserGroupMemberLinks {
	trimmedUser := strings.TrimSpace(username)
	if trimmedUser == "" {
		return nil
	}

	zoneForUser := strings.TrimSpace(userZone)
	if zoneForUser == "" {
		zoneForUser = strings.TrimSpace(groupZone)
	}

	return &domain.UserGroupMemberLinks{
		Self: &domain.ActionLink{
			Href:   userHref(trimmedUser, zoneForUser),
			Method: http.MethodGet,
		},
		RemoveFromGroup: &domain.ActionLink{
			Href:   userGroupMemberHref(groupName, trimmedUser, groupZone),
			Method: http.MethodDelete,
		},
	}
}

func userGroupHref(groupName string, zone string) string {
	query := url.Values{}
	if strings.TrimSpace(zone) != "" {
		query.Set("zone", strings.TrimSpace(zone))
	}

	href := "/api/v1/usergroup/" + url.PathEscape(strings.TrimSpace(groupName))
	if encoded := query.Encode(); encoded != "" {
		href += "?" + encoded
	}
	return href
}

func userGroupMemberBaseHref(groupName string, zone string) string {
	query := url.Values{}
	if strings.TrimSpace(zone) != "" {
		query.Set("zone", strings.TrimSpace(zone))
	}

	href := "/api/v1/usergroup/" + url.PathEscape(strings.TrimSpace(groupName)) + "/member"
	if encoded := query.Encode(); encoded != "" {
		href += "?" + encoded
	}
	return href
}

func userGroupMemberHref(groupName string, username string, zone string) string {
	query := url.Values{}
	if strings.TrimSpace(zone) != "" {
		query.Set("zone", strings.TrimSpace(zone))
	}

	href := "/api/v1/usergroup/" + url.PathEscape(strings.TrimSpace(groupName)) + "/member/" + url.PathEscape(strings.TrimSpace(username))
	if encoded := query.Encode(); encoded != "" {
		href += "?" + encoded
	}
	return href
}

func userGroupListHref(zone string, prefix string) string {
	query := url.Values{}
	if strings.TrimSpace(zone) != "" {
		query.Set("zone", strings.TrimSpace(zone))
	}
	if strings.TrimSpace(prefix) != "" {
		query.Set("prefix", strings.TrimSpace(prefix))
	}

	if encoded := query.Encode(); encoded != "" {
		return "/api/v1/usergroup?" + encoded
	}
	return "/api/v1/usergroup"
}

func writeUserGroupError(w http.ResponseWriter, err error) {
	if errors.Is(err, irods.ErrConflict) {
		writeError(w, http.StatusConflict, "conflict", err.Error())
		return
	}
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
