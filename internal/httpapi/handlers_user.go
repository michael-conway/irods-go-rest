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

type userUpdateRequest struct {
	Type     *string `json:"type"`
	Password *string `json:"password"`
}

type userCreateRequest struct {
	Name     string  `json:"name"`
	Type     *string `json:"type"`
	Password *string `json:"password"`
}

func (h *Handler) getUsers(w http.ResponseWriter, r *http.Request) {
	zone := h.userZoneFromRequest(r)
	userType := strings.TrimSpace(r.URL.Query().Get("type"))
	if userType != "" && !validRESTUserType(userType) {
		writeError(w, http.StatusBadRequest, "invalid_request", "type must be rodsuser or rodsadmin")
		return
	}

	prefix := strings.TrimSpace(r.URL.Query().Get("prefix"))
	if prefix != "" && len(prefix) < 3 {
		writeError(w, http.StatusBadRequest, "invalid_request", "prefix must be at least 3 characters")
		return
	}

	users, err := h.users.ListUsers(r.Context(), restservice.UserListOptions{
		Zone:   zone,
		Type:   userType,
		Prefix: prefix,
	})
	if err != nil {
		writeUserError(w, err)
		return
	}

	users = userResponseList(users)
	payload := map[string]any{
		"users": users,
		"count": len(users),
		"zone":  zone,
		"links": map[string]domain.ActionLink{
			"self": {
				Href:   userListHref(zone, userType, prefix),
				Method: http.MethodGet,
			},
			"create": {
				Href:   userListHref(zone, "", ""),
				Method: http.MethodPost,
			},
		},
	}
	if userType != "" {
		payload["type"] = userType
	}
	if prefix != "" {
		payload["prefix"] = prefix
	}

	writeJSON(w, http.StatusOK, payload)
}

func (h *Handler) getUser(w http.ResponseWriter, r *http.Request) {
	username := pathValue(r, "user_name")
	if username == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "user_name path parameter is required")
		return
	}

	user, err := h.users.GetUser(r.Context(), username, h.userZoneFromRequest(r))
	if err != nil {
		writeUserError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"user": userResponse(user),
	})
}

func (h *Handler) postUser(w http.ResponseWriter, r *http.Request) {
	var request userCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must be valid JSON")
		return
	}

	options, username, fields := userCreateOptionsFromRequest(h.userZoneFromRequest(r), request)
	if len(fields) > 0 {
		writeValidationError(w, http.StatusBadRequest, "invalid_request", "user create validation failed", fields)
		return
	}

	user, err := h.users.CreateUser(r.Context(), username, options)
	if err != nil {
		writeUserError(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"user": userResponse(user),
	})
}

func (h *Handler) putUser(w http.ResponseWriter, r *http.Request) {
	username := pathValue(r, "user_name")
	if username == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "user_name path parameter is required")
		return
	}

	var request userUpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must be valid JSON")
		return
	}

	options, fields := userUpdateOptionsFromRequest(h.userZoneFromRequest(r), request)
	if len(fields) > 0 {
		writeValidationError(w, http.StatusBadRequest, "invalid_request", "user update validation failed", fields)
		return
	}

	user, err := h.users.UpdateUser(r.Context(), username, options)
	if err != nil {
		writeUserError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"user": userResponse(user),
	})
}

func (h *Handler) deleteUser(w http.ResponseWriter, r *http.Request) {
	username := pathValue(r, "user_name")
	if username == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "user_name path parameter is required")
		return
	}

	if err := h.users.DeleteUser(r.Context(), username, h.userZoneFromRequest(r)); err != nil {
		writeUserError(w, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func userUpdateOptionsFromRequest(zone string, request userUpdateRequest) (restservice.UserUpdateOptions, map[string]string) {
	options := restservice.UserUpdateOptions{
		Zone: zone,
	}
	fields := map[string]string{}

	if request.Type != nil {
		userType := strings.TrimSpace(*request.Type)
		if !validRESTUserType(userType) {
			fields["type"] = "type must be rodsuser or rodsadmin"
		} else {
			options.Type = userType
			options.ChangeType = true
		}
	}

	if request.Password != nil {
		password := strings.TrimSpace(*request.Password)
		if password == "" {
			fields["password"] = "password cannot be empty"
		} else {
			options.Password = *request.Password
			options.ChangePassword = true
		}
	}

	if request.Type == nil && request.Password == nil {
		fields["request"] = "type or password is required"
	}

	if len(fields) == 0 {
		return options, nil
	}
	return options, fields
}

func userCreateOptionsFromRequest(
	zone string,
	request userCreateRequest,
) (restservice.UserCreateOptions, string, map[string]string) {
	options := restservice.UserCreateOptions{
		Zone: zone,
	}
	fields := map[string]string{}
	username := strings.TrimSpace(request.Name)
	if username == "" {
		fields["name"] = "name is required"
	}

	if request.Type == nil {
		fields["type"] = "type is required and must be rodsuser or rodsadmin"
	} else {
		userType := strings.TrimSpace(*request.Type)
		if !validRESTUserType(userType) {
			fields["type"] = "type must be rodsuser or rodsadmin"
		} else {
			options.Type = userType
		}
	}

	if request.Password != nil {
		password := strings.TrimSpace(*request.Password)
		if password == "" {
			fields["password"] = "password cannot be empty"
		} else {
			options.Password = *request.Password
		}
	}

	if len(fields) == 0 {
		return options, username, nil
	}
	return options, "", fields
}

func validRESTUserType(userType string) bool {
	switch strings.TrimSpace(userType) {
	case "rodsuser", "rodsadmin":
		return true
	default:
		return false
	}
}

func (h *Handler) userZoneFromRequest(r *http.Request) string {
	zone := strings.TrimSpace(r.URL.Query().Get("zone"))
	if zone != "" {
		return zone
	}
	return strings.TrimSpace(h.cfg.IrodsZone)
}

func userResponseList(users []domain.User) []domain.User {
	if len(users) == 0 {
		return []domain.User{}
	}

	mapped := make([]domain.User, 0, len(users))
	for _, user := range users {
		mapped = append(mapped, userResponse(user))
	}
	return mapped
}

func userResponse(user domain.User) domain.User {
	user.Links = userLinks(user)
	return user
}

func userLinks(user domain.User) *domain.UserLinks {
	if strings.TrimSpace(user.Name) == "" {
		return nil
	}

	href := userHref(user.Name, user.Zone)
	return &domain.UserLinks{
		Self: &domain.ActionLink{
			Href:   href,
			Method: http.MethodGet,
		},
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

func userHref(username string, zone string) string {
	query := url.Values{}
	if strings.TrimSpace(zone) != "" {
		query.Set("zone", strings.TrimSpace(zone))
	}

	href := "/api/v1/user/" + url.PathEscape(strings.TrimSpace(username))
	if encoded := query.Encode(); encoded != "" {
		href += "?" + encoded
	}
	return href
}

func userListHref(zone string, userType string, prefix string) string {
	query := url.Values{}
	if strings.TrimSpace(zone) != "" {
		query.Set("zone", strings.TrimSpace(zone))
	}
	if strings.TrimSpace(userType) != "" {
		query.Set("type", strings.TrimSpace(userType))
	}
	if strings.TrimSpace(prefix) != "" {
		query.Set("prefix", strings.TrimSpace(prefix))
	}

	if encoded := query.Encode(); encoded != "" {
		return "/api/v1/user?" + encoded
	}
	return "/api/v1/user"
}

func writeUserError(w http.ResponseWriter, err error) {
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
