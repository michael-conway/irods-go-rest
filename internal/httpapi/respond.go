package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/michael-conway/irods-go-rest/internal/requestctx"
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

func writeErrorFromErr(w http.ResponseWriter, r *http.Request, status int, code string, err error) {
	if err != nil {
		logArgs := []any{
			"status", status,
			"error_code", strings.TrimSpace(strings.ToLower(code)),
			"error_detail", err.Error(),
		}
		if r != nil {
			logArgs = append(logArgs, "method", r.Method, "path", r.URL.Path)
			if metadata, ok := requestctx.MetadataFromContext(r.Context()); ok && strings.TrimSpace(metadata.RequestID) != "" {
				logArgs = append(logArgs, "request_id", metadata.RequestID)
			}
		}

		if status >= http.StatusInternalServerError {
			slog.Error("request failed", logArgs...)
		} else {
			slog.Warn("request failed", logArgs...)
		}
	}

	writeError(w, status, code, publicErrorMessage(status, code))
}

func publicErrorMessage(status int, code string) string {
	switch strings.TrimSpace(strings.ToLower(code)) {
	case "not_found":
		return "resource not found"
	case "permission_denied":
		return "permission denied"
	case "conflict":
		return "request conflicts with current resource state"
	case "invalid_range":
		return "invalid range requested"
	case "invalid_request":
		return "invalid request"
	case "not_supported":
		return "operation not supported"
	case "not_configured":
		return "operation is not configured"
	case "auth_not_configured":
		return "authentication is not configured"
	case "auth_failed":
		return "authentication failed"
	case "invalid_callback":
		return "invalid callback request"
	case "internal_error":
		return "internal server error"
	}

	if status >= http.StatusInternalServerError {
		return "internal server error"
	}
	if status >= http.StatusBadRequest {
		return "request failed"
	}
	return "request failed"
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
