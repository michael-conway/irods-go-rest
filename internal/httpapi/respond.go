package httpapi

import (
	"encoding/json"
	"net/http"
	"strings"
)

type errorResponse struct {
	Code    string            `json:"code"`
	Message string            `json:"message"`
	Fields  map[string]string `json:"fields,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, code string, message string) {
	setRequestErrorMetadata(w, code, errorClassFromResponse(status, code))
	writeJSON(w, status, errorResponse{
		Code:    code,
		Message: message,
	})
}

func writeValidationError(w http.ResponseWriter, status int, code string, message string, fields map[string]string) {
	setRequestErrorMetadata(w, code, errorClassFromResponse(status, code))
	writeJSON(w, status, errorResponse{
		Code:    code,
		Message: message,
		Fields:  fields,
	})
}

func writeHTML(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body))
}

func errorClassFromResponse(status int, code string) string {
	normalizedCode := strings.TrimSpace(strings.ToLower(code))
	switch normalizedCode {
	case "missing_authorization", "invalid_authorization", "invalid_token", "auth_not_configured", "auth_failed":
		return "auth_error"
	case "permission_denied":
		return "permission_denied"
	case "not_found":
		return "not_found"
	case "conflict":
		return "conflict"
	case "invalid_range":
		return "invalid_range"
	case "invalid_request":
		return "validation_error"
	case "unsupported":
		return "unsupported"
	case "service_unavailable":
		return "service_unavailable"
	case "internal_error":
		return "internal_error"
	}

	return defaultErrorClassForStatus(status)
}
