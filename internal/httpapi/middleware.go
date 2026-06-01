package httpapi

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/michael-conway/irods-go-rest/internal/auth"
	"github.com/michael-conway/irods-go-rest/internal/requestctx"
)

const defaultCORSAllowedMethods = "GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS"
const defaultCORSAllowedHeaders = "Authorization, Content-Type, Accept, X-Request-ID, X-IRODS-Source, X-IRODS-Actor, Idempotency-Key"

const (
	requestIDHeader      = "X-Request-ID"
	requestSourceHeader  = "X-IRODS-Source"
	requestActorHeader   = "X-IRODS-Actor"
	idempotencyKeyHeader = "Idempotency-Key"
)

func corsMiddleware(allowedOrigins []string, next http.Handler) http.Handler {
	allowed := normalizedAllowedOrigins(allowedOrigins)
	if len(allowed) == 0 {
		return next
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := strings.TrimSpace(r.Header.Get("Origin"))
		allowOrigin := allowedCORSOrigin(origin, allowed)
		if allowOrigin != "" {
			header := w.Header()
			header.Set("Access-Control-Allow-Origin", allowOrigin)
			header.Add("Vary", "Origin")
			header.Set("Access-Control-Allow-Methods", defaultCORSAllowedMethods)

			header.Set("Access-Control-Allow-Headers", defaultCORSAllowedHeaders)
			header.Set("Access-Control-Max-Age", "600")
		}

		if r.Method == http.MethodOptions && strings.TrimSpace(r.Header.Get("Access-Control-Request-Method")) != "" {
			if origin != "" && allowOrigin == "" {
				http.Error(w, "CORS origin is not allowed", http.StatusForbidden)
				return
			}

			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func normalizedAllowedOrigins(origins []string) map[string]string {
	allowed := map[string]string{}
	for _, origin := range origins {
		canonicalOrigin := canonicalCORSOrigin(origin)
		if canonicalOrigin == "" {
			continue
		}
		allowed[canonicalOrigin] = canonicalOrigin
	}
	return allowed
}

func allowedCORSOrigin(origin string, allowed map[string]string) string {
	canonicalOrigin := canonicalCORSOrigin(origin)
	if canonicalOrigin == "" {
		return ""
	}

	if allowed["*"] == "*" {
		return "*"
	}
	if allowedOrigin := allowed[canonicalOrigin]; allowedOrigin != "" {
		return allowedOrigin
	}
	return ""
}

func canonicalCORSOrigin(origin string) string {
	origin = strings.TrimRight(strings.TrimSpace(origin), "/")
	if origin == "" {
		return ""
	}
	if origin == "*" {
		return "*"
	}
	parsed, err := url.Parse(origin)
	if err != nil {
		return ""
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return ""
	}
	if parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return ""
	}
	if parsed.Path != "" && parsed.Path != "/" {
		return ""
	}
	return parsed.Scheme + "://" + parsed.Host
}

func requestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		metadata := requestMetadataFromRequest(r)
		r = r.WithContext(requestctx.WithMetadata(r.Context(), metadata))

		lrw := newLoggingResponseWriter(w)
		if metadata.RequestID != "" {
			lrw.Header().Set(requestIDHeader, metadata.RequestID)
		}

		next.ServeHTTP(lrw, r)

		status := lrw.StatusCode()
		duration := time.Since(start)

		route := strings.TrimSpace(r.Pattern)
		if route == "" {
			route = r.Method + " " + r.URL.Path
		}

		authMode, principal := requestAuthSummary(r, lrw)
		errorClass := lrw.errorClass
		if errorClass == "" && status >= http.StatusBadRequest {
			errorClass = defaultErrorClassForStatus(status)
		}

		logArgs := []any{
			"method", r.Method,
			"route", route,
			"path", r.URL.Path,
			"status", status,
			"duration_ms", duration.Milliseconds(),
			"auth_mode", authMode,
			"principal", principal,
			"response_bytes", lrw.bytesWritten,
		}

		logArgs = append(logArgs, requestAuditLogFields(r, lrw)...)
		logArgs = append(logArgs, requestOperationIdentifiers(r)...)
		if lrw.errorCode != "" {
			logArgs = append(logArgs, "error_code", lrw.errorCode)
		}
		if errorClass != "" {
			logArgs = append(logArgs, "error_class", errorClass)
		}

		if status >= http.StatusInternalServerError {
			slog.Error("http request completed", logArgs...)
			return
		}
		if status >= http.StatusBadRequest {
			slog.Warn("http request completed", logArgs...)
			return
		}
		slog.Info("http request completed", logArgs...)
	})
}

func requestMetadataFromRequest(r *http.Request) requestctx.Metadata {
	requestID := strings.TrimSpace(r.Header.Get(requestIDHeader))
	if requestID == "" {
		requestID = newRequestID()
	}

	return requestctx.Metadata{
		RequestID:      requestID,
		Source:         strings.TrimSpace(r.Header.Get(requestSourceHeader)),
		Actor:          strings.TrimSpace(r.Header.Get(requestActorHeader)),
		IdempotencyKey: strings.TrimSpace(r.Header.Get(idempotencyKeyHeader)),
	}
}

func newRequestID() string {
	var buffer [16]byte
	if _, err := rand.Read(buffer[:]); err == nil {
		return hex.EncodeToString(buffer[:])
	}
	return strconv.FormatInt(time.Now().UnixNano(), 36)
}

func requestAuditLogFields(r *http.Request, w *loggingResponseWriter) []any {
	fields := []any{}

	if metadata, ok := requestctx.MetadataFromContext(r.Context()); ok {
		fields = appendNonEmptyLogField(fields, "request_id", metadata.RequestID)
		fields = appendNonEmptyLogField(fields, "request_source", metadata.Source)
		fields = appendNonEmptyLogField(fields, "request_actor", metadata.Actor)
		fields = appendNonEmptyLogField(fields, "idempotency_key", metadata.IdempotencyKey)
	}

	if w != nil && (w.authSubject != "" || w.authClientID != "" || w.authScope != "" || w.authAudience != "") {
		fields = appendNonEmptyLogField(fields, "auth_subject", w.authSubject)
		fields = appendNonEmptyLogField(fields, "auth_client_id", w.authClientID)
		fields = appendNonEmptyLogField(fields, "auth_scope", w.authScope)
		fields = appendNonEmptyLogField(fields, "auth_audience", w.authAudience)
		return fields
	}

	if principal, ok := auth.PrincipalFromContext(r.Context()); ok {
		fields = appendPrincipalLogFields(fields, principal)
	}

	return fields
}

func appendPrincipalLogFields(fields []any, principal auth.Principal) []any {
	fields = appendNonEmptyLogField(fields, "auth_subject", principal.Subject)
	fields = appendNonEmptyLogField(fields, "auth_client_id", principal.ClientID)
	fields = appendNonEmptyLogField(fields, "auth_scope", strings.Join(principal.Scope, " "))
	fields = appendNonEmptyLogField(fields, "auth_audience", strings.Join(principal.Audience, " "))
	return fields
}

func appendNonEmptyLogField(fields []any, key string, value string) []any {
	value = strings.TrimSpace(value)
	if value == "" {
		return fields
	}
	return append(fields, key, value)
}

type requestLogMetadataRecorder interface {
	setRequestAuth(mode string, principal string)
	setRequestPrincipal(principal auth.Principal)
	setRequestError(code string, class string)
}

type loggingResponseWriter struct {
	http.ResponseWriter
	statusCode   int
	bytesWritten int
	authMode     string
	principal    string
	authSubject  string
	authClientID string
	authScope    string
	authAudience string
	errorCode    string
	errorClass   string
}

func newLoggingResponseWriter(w http.ResponseWriter) *loggingResponseWriter {
	return &loggingResponseWriter{
		ResponseWriter: w,
		statusCode:     http.StatusOK,
	}
}

func (w *loggingResponseWriter) StatusCode() int {
	if w.statusCode <= 0 {
		return http.StatusOK
	}
	return w.statusCode
}

func (w *loggingResponseWriter) setRequestAuth(mode string, principal string) {
	mode = strings.TrimSpace(mode)
	principal = strings.TrimSpace(principal)

	if mode != "" {
		w.authMode = mode
	}
	if principal != "" {
		w.principal = principal
	}
}

func (w *loggingResponseWriter) setRequestPrincipal(principal auth.Principal) {
	w.authSubject = strings.TrimSpace(principal.Subject)
	w.authClientID = strings.TrimSpace(principal.ClientID)
	w.authScope = strings.Join(principal.Scope, " ")
	w.authAudience = strings.Join(principal.Audience, " ")
}

func (w *loggingResponseWriter) setRequestError(code string, class string) {
	code = strings.TrimSpace(code)
	class = strings.TrimSpace(class)

	if code != "" {
		w.errorCode = code
	}
	if class != "" {
		w.errorClass = class
	}
}

func (w *loggingResponseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *loggingResponseWriter) Write(data []byte) (int, error) {
	if w.statusCode <= 0 {
		w.statusCode = http.StatusOK
	}

	n, err := w.ResponseWriter.Write(data)
	w.bytesWritten += n
	return n, err
}

func (w *loggingResponseWriter) Flush() {
	flusher, ok := w.ResponseWriter.(http.Flusher)
	if ok {
		flusher.Flush()
	}
}

func (w *loggingResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("hijack not supported")
	}
	return hijacker.Hijack()
}

func (w *loggingResponseWriter) Push(target string, opts *http.PushOptions) error {
	pusher, ok := w.ResponseWriter.(http.Pusher)
	if !ok {
		return http.ErrNotSupported
	}
	return pusher.Push(target, opts)
}

func (w *loggingResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func setRequestAuthMetadata(w http.ResponseWriter, mode string, principal string) {
	recorder, ok := w.(requestLogMetadataRecorder)
	if !ok {
		return
	}
	recorder.setRequestAuth(mode, principal)
}

func setRequestPrincipalMetadata(w http.ResponseWriter, principal auth.Principal) {
	recorder, ok := w.(requestLogMetadataRecorder)
	if !ok {
		return
	}
	recorder.setRequestPrincipal(principal)
}

func setRequestErrorMetadata(w http.ResponseWriter, code string, class string) {
	recorder, ok := w.(requestLogMetadataRecorder)
	if !ok {
		return
	}
	recorder.setRequestError(code, class)
}

func requestAuthSummary(r *http.Request, w *loggingResponseWriter) (string, string) {
	if w != nil {
		if strings.TrimSpace(w.authMode) != "" || strings.TrimSpace(w.principal) != "" {
			return strings.TrimSpace(w.authMode), strings.TrimSpace(w.principal)
		}
	}

	if ticket, ok := auth.TicketFromContext(r.Context()); ok && strings.TrimSpace(ticket) != "" {
		return "ticket", ""
	}

	if principal, ok := auth.PrincipalFromContext(r.Context()); ok {
		username := strings.TrimSpace(principal.Username)
		if username == "" {
			username = strings.TrimSpace(principal.Subject)
		}
		for _, scope := range principal.Scope {
			if strings.EqualFold(strings.TrimSpace(scope), "basic") {
				return "basic", username
			}
		}
		return "bearer", username
	}

	authorization := strings.TrimSpace(r.Header.Get("Authorization"))
	if authorization == "" {
		return "none", ""
	}

	scheme, _, found := strings.Cut(authorization, " ")
	if !found {
		return "invalid", ""
	}

	switch strings.ToLower(strings.TrimSpace(scheme)) {
	case "basic":
		return "basic", ""
	case "bearer":
		return "bearer", ""
	default:
		return "invalid", ""
	}
}

func requestOperationIdentifiers(r *http.Request) []any {
	ids := []any{}

	if irodsPath := strings.TrimSpace(r.URL.Query().Get("irods_path")); irodsPath != "" {
		ids = append(ids, "irods_path", irodsPath)
	}
	if absolutePath := strings.TrimSpace(r.URL.Query().Get("absolute_path")); absolutePath != "" {
		ids = append(ids, "absolute_path", absolutePath)
	}
	if scope := strings.TrimSpace(r.URL.Query().Get("scope")); scope != "" {
		ids = append(ids, "scope", scope)
	}
	if searchScope := strings.TrimSpace(r.URL.Query().Get("search_scope")); searchScope != "" {
		ids = append(ids, "search_scope", searchScope)
	}
	if recursive := strings.TrimSpace(r.URL.Query().Get("recursive")); recursive != "" {
		ids = append(ids, "recursive", recursive)
	}

	pathKeys := []string{
		"resource_id",
		"user_name",
		"group_name",
		"ticket_name",
		"bucket_id",
		"avu_id",
		"acl_id",
	}
	for _, key := range pathKeys {
		if value := strings.TrimSpace(r.PathValue(key)); value != "" {
			ids = append(ids, key, value)
		}
	}

	return ids
}

func defaultErrorClassForStatus(status int) string {
	switch status {
	case http.StatusBadRequest:
		return "validation_error"
	case http.StatusUnauthorized:
		return "auth_error"
	case http.StatusForbidden:
		return "permission_denied"
	case http.StatusNotFound:
		return "not_found"
	case http.StatusConflict:
		return "conflict"
	case http.StatusNotImplemented:
		return "unsupported"
	case http.StatusServiceUnavailable:
		return "service_unavailable"
	default:
		if status >= 500 {
			return "internal_error"
		}
		if status >= 400 {
			return "client_error_" + strconv.Itoa(status)
		}
		return ""
	}
}
