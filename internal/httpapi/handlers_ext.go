package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"

	s3adminext "github.com/michael-conway/go-irodsclient-extensions/s3admin"
	"github.com/michael-conway/irods-go-rest/internal/domain"
	"github.com/michael-conway/irods-go-rest/internal/irods"
)

func (h *Handler) getExtFavorites(w http.ResponseWriter, r *http.Request) {
	favorites, err := h.paths.ListFavorites(r.Context())
	if err != nil {
		writePathError(w, err)
		return
	}

	filterPath := strings.TrimSpace(r.URL.Query().Get("absolute_path"))
	if filterPath != "" {
		filterPath, err = normalizeExtFavoritePath(filterPath)
		if err != nil {
			writeValidationError(w, http.StatusBadRequest, "invalid_request", "favorite request validation failed", map[string]string{
				"absolute_path": "absolute_path must be an absolute iRODS path",
			})
			return
		}

		filtered := make([]domain.Favorite, 0, len(favorites))
		for _, favorite := range favorites {
			if path.Clean(favorite.AbsolutePath) == filterPath {
				filtered = append(filtered, favorite)
			}
		}
		if len(filtered) == 0 {
			writeError(w, http.StatusNotFound, "not_found", "favorite not found")
			return
		}
		favorites = filtered
	}

	mappedFavorites := favoriteResponseList(favorites)
	writeJSON(w, http.StatusOK, map[string]any{
		"favorites": mappedFavorites,
		"count":     len(mappedFavorites),
		"links":     favoriteCollectionLinks(r),
	})
}

func (h *Handler) postExtFavorite(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Name         string `json:"name"`
		AbsolutePath string `json:"absolute_path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must be valid JSON")
		return
	}

	if fields := favoriteValidationFields(request.Name, request.AbsolutePath); len(fields) > 0 {
		writeValidationError(w, http.StatusBadRequest, "invalid_request", "favorite request validation failed", fields)
		return
	}

	favorite, err := h.paths.AddFavorite(r.Context(), request.Name, request.AbsolutePath)
	if err != nil {
		writePathError(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"favorite": favoriteResponse(favorite),
	})
}

func (h *Handler) putExtFavorite(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Name         string `json:"name"`
		AbsolutePath string `json:"absolute_path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must be valid JSON")
		return
	}

	if fields := favoriteValidationFields(request.Name, request.AbsolutePath); len(fields) > 0 {
		writeValidationError(w, http.StatusBadRequest, "invalid_request", "favorite request validation failed", fields)
		return
	}

	favorite, err := h.paths.RenameFavorite(r.Context(), request.AbsolutePath, request.Name)
	if err != nil {
		if errorsIsNotFound(err) {
			writeError(w, http.StatusNotFound, "not_found", err.Error())
			return
		}
		writePathError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"favorite": favoriteResponse(favorite),
	})
}

func (h *Handler) deleteExtFavorite(w http.ResponseWriter, r *http.Request) {
	var request struct {
		AbsolutePath string `json:"absolute_path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must be valid JSON")
		return
	}

	pathValue := strings.TrimSpace(request.AbsolutePath)
	if pathValue == "" {
		writeValidationError(w, http.StatusBadRequest, "invalid_request", "favorite request validation failed", map[string]string{
			"absolute_path": "absolute_path is required",
		})
		return
	}
	if _, err := normalizeExtFavoritePath(pathValue); err != nil {
		writeValidationError(w, http.StatusBadRequest, "invalid_request", "favorite request validation failed", map[string]string{
			"absolute_path": "absolute_path must be an absolute iRODS path",
		})
		return
	}

	if err := h.paths.RemoveFavorite(r.Context(), pathValue); err != nil {
		writePathError(w, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) getExtS3Buckets(w http.ResponseWriter, r *http.Request) {
	if !h.requireS3APISupported(w) {
		return
	}

	options, fields := s3BucketListOptionsFromRequest(r, true)
	if len(fields) > 0 {
		writeValidationError(w, http.StatusBadRequest, "invalid_request", "s3 bucket request validation failed", fields)
		return
	}

	buckets, err := h.s3Admin.ListBuckets(r.Context(), options)
	if err != nil {
		writeS3AdminError(w, err)
		return
	}

	mappedBuckets := s3BucketResponseList(buckets)
	writeJSON(w, http.StatusOK, map[string]any{
		"buckets": mappedBuckets,
		"count":   len(mappedBuckets),
		"links":   s3BucketCollectionLinks(r),
	})
}

func (h *Handler) getExtS3Bucket(w http.ResponseWriter, r *http.Request) {
	if !h.requireS3APISupported(w) {
		return
	}

	bucketID := pathValue(r, "bucket_id")
	if bucketID == "" {
		writeValidationError(w, http.StatusBadRequest, "invalid_request", "s3 bucket request validation failed", map[string]string{
			"bucket_id": "bucket_id is required",
		})
		return
	}

	options, fields := s3BucketListOptionsFromRequest(r, true)
	if len(fields) > 0 {
		writeValidationError(w, http.StatusBadRequest, "invalid_request", "s3 bucket request validation failed", fields)
		return
	}

	bucket, err := h.s3Admin.GetBucket(r.Context(), bucketID, options)
	if err != nil {
		writeS3AdminError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"bucket": s3BucketResponse(bucket),
	})
}

func (h *Handler) getExtS3BucketByPath(w http.ResponseWriter, r *http.Request) {
	if !h.requireS3APISupported(w) {
		return
	}

	irodsPath := strings.TrimSpace(r.URL.Query().Get("irods_path"))
	if _, err := normalizeExtS3Path(irodsPath); err != nil {
		writeValidationError(w, http.StatusBadRequest, "invalid_request", "s3 bucket request validation failed", map[string]string{
			"irods_path": "irods_path must be an absolute iRODS path",
		})
		return
	}

	bucket, err := h.s3Admin.GetBucketByPath(r.Context(), irodsPath)
	if err != nil {
		writeS3AdminError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"bucket": s3BucketResponse(bucket),
	})
}

func (h *Handler) postExtS3Bucket(w http.ResponseWriter, r *http.Request) {
	if !h.requireS3APISupported(w) {
		return
	}

	h.upsertExtS3Bucket(w, r)
}

func (h *Handler) putExtS3Bucket(w http.ResponseWriter, r *http.Request) {
	if !h.requireS3APISupported(w) {
		return
	}

	h.upsertExtS3Bucket(w, r)
}

func (h *Handler) upsertExtS3Bucket(w http.ResponseWriter, r *http.Request) {
	var request struct {
		IRODSPath    string `json:"irods_path"`
		BucketName   string `json:"bucket_name"`
		AutoGenerate bool   `json:"auto_generate"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must be valid JSON")
		return
	}

	fields := map[string]string{}
	irodsPath, err := normalizeExtS3Path(request.IRODSPath)
	if err != nil {
		fields["irods_path"] = "irods_path must be an absolute iRODS path"
	}

	bucketName := strings.TrimSpace(request.BucketName)
	if bucketName == "" && !request.AutoGenerate {
		fields["bucket_name"] = "bucket_name is required unless auto_generate is true"
	}

	if len(fields) > 0 {
		writeValidationError(w, http.StatusBadRequest, "invalid_request", "s3 bucket request validation failed", fields)
		return
	}

	bucket, created, err := h.s3Admin.UpsertBucket(r.Context(), irodsPath, irods.S3BucketUpsertOptions{
		BucketName:   bucketName,
		AutoGenerate: request.AutoGenerate,
	})
	if err != nil {
		writeS3AdminError(w, err)
		return
	}

	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, map[string]any{
		"bucket": s3BucketResponse(bucket),
	})
}

func (h *Handler) deleteExtS3Bucket(w http.ResponseWriter, r *http.Request) {
	if !h.requireS3APISupported(w) {
		return
	}

	bucketID := pathValue(r, "bucket_id")
	if bucketID == "" {
		writeValidationError(w, http.StatusBadRequest, "invalid_request", "s3 bucket request validation failed", map[string]string{
			"bucket_id": "bucket_id is required",
		})
		return
	}

	if err := h.s3Admin.DeleteBucket(r.Context(), bucketID); err != nil {
		writeS3AdminError(w, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) postExtS3BucketMappingRefresh(w http.ResponseWriter, r *http.Request) {
	if !h.requireS3APISupported(w) {
		return
	}

	result, err := h.s3Admin.RebuildBucketMapping(r.Context())
	if err != nil {
		writeS3AdminError(w, err)
		return
	}

	result.Buckets = s3BucketResponseList(result.Buckets)
	result.Count = len(result.Buckets)
	writeJSON(w, http.StatusOK, map[string]any{
		"bucket_mapping": result,
	})
}

func (h *Handler) getExtS3UserSecrets(w http.ResponseWriter, r *http.Request) {
	if !h.requireS3APISupported(w) {
		return
	}

	userSecrets, err := h.s3Admin.ListUserSecrets(r.Context())
	if err != nil {
		writeS3AdminError(w, err)
		return
	}

	mappedUserSecrets := s3UserSecretResponseList(userSecrets)
	writeJSON(w, http.StatusOK, map[string]any{
		"user_secrets": mappedUserSecrets,
		"count":        len(mappedUserSecrets),
	})
}

func (h *Handler) getExtS3UserSecret(w http.ResponseWriter, r *http.Request) {
	if !h.requireS3APISupported(w) {
		return
	}

	userName := pathValue(r, "user_name")
	if userName == "" {
		writeValidationError(w, http.StatusBadRequest, "invalid_request", "s3 user secret request validation failed", map[string]string{
			"user_name": "user_name is required",
		})
		return
	}

	userSecret, err := h.s3Admin.GetUserSecret(r.Context(), userName)
	if err != nil {
		writeS3AdminError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"user_secret": s3UserSecretResponse(userSecret),
	})
}

func (h *Handler) postExtS3UserSecret(w http.ResponseWriter, r *http.Request) {
	if !h.requireS3APISupported(w) {
		return
	}

	h.storeExtS3UserSecret(w, r, http.StatusCreated)
}

func (h *Handler) putExtS3UserSecret(w http.ResponseWriter, r *http.Request) {
	if !h.requireS3APISupported(w) {
		return
	}

	h.storeExtS3UserSecret(w, r, http.StatusOK)
}

func (h *Handler) storeExtS3UserSecret(w http.ResponseWriter, r *http.Request, status int) {
	var request struct {
		UserName     string `json:"user_name"`
		SecretKey    string `json:"secret_key"`
		AutoGenerate bool   `json:"auto_generate"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must be valid JSON")
		return
	}

	fields := s3UserSecretValidationFields(request.UserName, request.SecretKey, request.AutoGenerate)
	if len(fields) > 0 {
		writeValidationError(w, http.StatusBadRequest, "invalid_request", "s3 user secret request validation failed", fields)
		return
	}

	userSecret, err := h.s3Admin.StoreUserSecret(r.Context(), request.UserName, irods.S3UserSecretStoreOptions{
		SecretKey:    request.SecretKey,
		AutoGenerate: request.AutoGenerate,
	})
	if err != nil {
		writeS3AdminError(w, err)
		return
	}

	writeJSON(w, status, map[string]any{
		"user_secret": s3UserSecretResponse(userSecret),
	})
}

func (h *Handler) deleteExtS3UserSecret(w http.ResponseWriter, r *http.Request) {
	if !h.requireS3APISupported(w) {
		return
	}

	userName := pathValue(r, "user_name")
	if userName == "" {
		writeValidationError(w, http.StatusBadRequest, "invalid_request", "s3 user secret request validation failed", map[string]string{
			"user_name": "user_name is required",
		})
		return
	}

	if err := h.s3Admin.DeleteUserSecret(r.Context(), userName); err != nil {
		writeS3AdminError(w, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) postExtS3UserMappingRefresh(w http.ResponseWriter, r *http.Request) {
	if !h.requireS3APISupported(w) {
		return
	}

	result, err := h.s3Admin.RebuildUserMapping(r.Context())
	if err != nil {
		writeS3AdminError(w, err)
		return
	}

	result.Users = s3UserSecretResponseList(result.Users)
	result.Count = len(result.Users)
	writeJSON(w, http.StatusOK, map[string]any{
		"user_mapping": result,
	})
}

func favoriteValidationFields(name string, favoritePath string) map[string]string {
	fields := map[string]string{}

	if strings.TrimSpace(name) == "" {
		fields["name"] = "name is required"
	}
	if strings.TrimSpace(favoritePath) == "" {
		fields["absolute_path"] = "absolute_path is required"
	} else if _, err := normalizeExtFavoritePath(favoritePath); err != nil {
		fields["absolute_path"] = "absolute_path must be an absolute iRODS path"
	}

	if len(fields) == 0 {
		return nil
	}
	return fields
}

func s3UserSecretValidationFields(userName string, secretKey string, autoGenerate bool) map[string]string {
	fields := map[string]string{}

	if strings.TrimSpace(userName) == "" {
		fields["user_name"] = "user_name is required"
	}
	if strings.Contains(strings.TrimSpace(userName), "/") {
		fields["user_name"] = "user_name must not contain path separators"
	}
	if strings.TrimSpace(secretKey) == "" && !autoGenerate {
		fields["secret_key"] = "secret_key is required unless auto_generate is true"
	}

	if len(fields) == 0 {
		return nil
	}
	return fields
}

func normalizeExtFavoritePath(favoritePath string) (string, error) {
	favoritePath = strings.TrimSpace(favoritePath)
	if favoritePath == "" || !path.IsAbs(favoritePath) {
		return "", fmt.Errorf("favorite path must be absolute")
	}

	cleaned := path.Clean(favoritePath)
	if cleaned == "." || cleaned == "/" {
		return "", fmt.Errorf("favorite path must be absolute")
	}
	return cleaned, nil
}

func normalizeExtS3Path(irodsPath string) (string, error) {
	irodsPath = strings.TrimSpace(irodsPath)
	if irodsPath == "" || !path.IsAbs(irodsPath) {
		return "", fmt.Errorf("s3 bucket path must be absolute")
	}

	cleaned := path.Clean(irodsPath)
	if cleaned == "." || cleaned == "/" {
		return "", fmt.Errorf("s3 bucket path must be absolute")
	}
	return cleaned, nil
}

func s3BucketListOptionsFromRequest(r *http.Request, defaultRecursive bool) (irods.S3BucketListOptions, map[string]string) {
	fields := map[string]string{}
	query := r.URL.Query()

	irodsPath := strings.TrimSpace(query.Get("irods_path"))
	if irodsPath != "" {
		normalizedPath, err := normalizeExtS3Path(irodsPath)
		if err != nil {
			fields["irods_path"] = "irods_path must be an absolute iRODS path"
		} else {
			irodsPath = normalizedPath
		}
	}

	recursive := defaultRecursive
	if rawRecursive := strings.TrimSpace(query.Get("recursive")); rawRecursive != "" {
		parsed, err := strconv.ParseBool(rawRecursive)
		if err != nil {
			fields["recursive"] = "recursive must be true or false"
		} else {
			recursive = parsed
		}
	}

	if len(fields) > 0 {
		return irods.S3BucketListOptions{}, fields
	}

	return irods.S3BucketListOptions{
		IRODSPath:  irodsPath,
		BucketName: strings.TrimSpace(query.Get("bucket_name")),
		Recursive:  recursive,
	}, nil
}

func favoriteResponseList(favorites []domain.Favorite) []domain.Favorite {
	if len(favorites) == 0 {
		return []domain.Favorite{}
	}

	mapped := make([]domain.Favorite, 0, len(favorites))
	for _, favorite := range favorites {
		mapped = append(mapped, favoriteResponse(favorite))
	}
	return mapped
}

func favoriteResponse(favorite domain.Favorite) domain.Favorite {
	favorite.Links = &domain.FavoriteLinks{
		Self: &domain.ActionLink{
			Href:   "/api/v1/ext/favorites?absolute_path=" + url.QueryEscape(favorite.AbsolutePath),
			Method: http.MethodGet,
		},
		Details: &domain.ActionLink{
			Href:   "/api/v1/path?irods_path=" + url.QueryEscape(favorite.AbsolutePath),
			Method: http.MethodGet,
		},
		Update: &domain.ActionLink{
			Href:   "/api/v1/ext/favorites",
			Method: http.MethodPut,
		},
		Delete: &domain.ActionLink{
			Href:   "/api/v1/ext/favorites",
			Method: http.MethodDelete,
		},
	}

	return favorite
}

func favoriteCollectionLinks(r *http.Request) *domain.FavoriteCollectionLinks {
	links := &domain.FavoriteCollectionLinks{
		Self: actionLinkFromRequest(r),
		Create: &domain.ActionLink{
			Href:   "/api/v1/ext/favorites",
			Method: http.MethodPost,
		},
		Update: &domain.ActionLink{
			Href:   "/api/v1/ext/favorites",
			Method: http.MethodPut,
		},
		Delete: &domain.ActionLink{
			Href:   "/api/v1/ext/favorites",
			Method: http.MethodDelete,
		},
	}

	if links.Self == nil {
		links.Self = &domain.ActionLink{
			Href:   "/api/v1/ext/favorites",
			Method: http.MethodGet,
		}
	}

	return links
}

func s3BucketResponseList(buckets []domain.S3Bucket) []domain.S3Bucket {
	if len(buckets) == 0 {
		return []domain.S3Bucket{}
	}

	mapped := make([]domain.S3Bucket, 0, len(buckets))
	for _, bucket := range buckets {
		mapped = append(mapped, s3BucketResponse(bucket))
	}
	return mapped
}

func s3BucketResponse(bucket domain.S3Bucket) domain.S3Bucket {
	bucket.Links = &domain.S3BucketLinks{
		Self: &domain.ActionLink{
			Href:   "/api/v1/ext/s3/buckets/" + url.PathEscape(bucket.BucketID),
			Method: http.MethodGet,
		},
		Path: &domain.ActionLink{
			Href:   "/api/v1/ext/s3/buckets/by-path?irods_path=" + url.QueryEscape(bucket.IRODSPath),
			Method: http.MethodGet,
		},
		Details: &domain.ActionLink{
			Href:   "/api/v1/path?irods_path=" + url.QueryEscape(bucket.IRODSPath),
			Method: http.MethodGet,
		},
		Update: &domain.ActionLink{
			Href:   "/api/v1/ext/s3/buckets",
			Method: http.MethodPut,
		},
		Delete: &domain.ActionLink{
			Href:   "/api/v1/ext/s3/buckets/" + url.PathEscape(bucket.BucketID),
			Method: http.MethodDelete,
		},
	}
	return bucket
}

func s3BucketCollectionLinks(r *http.Request) *domain.S3BucketCollectionLinks {
	links := &domain.S3BucketCollectionLinks{
		Self: actionLinkFromRequest(r),
		Create: &domain.ActionLink{
			Href:   "/api/v1/ext/s3/buckets",
			Method: http.MethodPost,
		},
		Update: &domain.ActionLink{
			Href:   "/api/v1/ext/s3/buckets",
			Method: http.MethodPut,
		},
	}
	if links.Self == nil {
		links.Self = &domain.ActionLink{
			Href:   "/api/v1/ext/s3/buckets",
			Method: http.MethodGet,
		}
	}
	return links
}

func s3UserSecretResponseList(users []domain.S3UserSecret) []domain.S3UserSecret {
	if len(users) == 0 {
		return []domain.S3UserSecret{}
	}

	mapped := make([]domain.S3UserSecret, 0, len(users))
	for _, user := range users {
		mapped = append(mapped, s3UserSecretResponse(user))
	}
	return mapped
}

func s3UserSecretResponse(userSecret domain.S3UserSecret) domain.S3UserSecret {
	userName := strings.TrimSpace(userSecret.UserName)
	userSecret.Links = &domain.S3UserSecretLinks{
		Self: &domain.ActionLink{
			Href:   "/api/v1/ext/s3/user-secrets/" + url.PathEscape(userName),
			Method: http.MethodGet,
		},
		Update: &domain.ActionLink{
			Href:   "/api/v1/ext/s3/user-secrets",
			Method: http.MethodPut,
		},
		Delete: &domain.ActionLink{
			Href:   "/api/v1/ext/s3/user-secrets/" + url.PathEscape(userName),
			Method: http.MethodDelete,
		},
	}
	return userSecret
}

func (h *Handler) requireS3APISupported(w http.ResponseWriter) bool {
	if h.cfg.S3ApiSupported {
		return true
	}

	writeError(w, http.StatusNotImplemented, "not_supported", "s3 api extension operations are not supported")
	return false
}

func writeS3AdminError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, irods.ErrS3AdminNotSupported):
		writeError(w, http.StatusNotImplemented, "not_supported", err.Error())
	case errors.Is(err, irods.ErrS3AdminNotConfigured):
		writeError(w, http.StatusServiceUnavailable, "not_configured", err.Error())
	case errors.Is(err, irods.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", err.Error())
	case errors.Is(err, irods.ErrPermissionDenied):
		writeError(w, http.StatusForbidden, "permission_denied", err.Error())
	case errors.Is(err, irods.ErrConflict), errors.Is(err, s3adminext.ErrDuplicateBucket), errors.Is(err, s3adminext.ErrBucketAlreadySet), errors.Is(err, s3adminext.ErrDuplicateUserMapping):
		writeError(w, http.StatusConflict, "conflict", err.Error())
	case errors.Is(err, s3adminext.ErrInvalidBucketName),
		errors.Is(err, s3adminext.ErrInvalidIRODSPath),
		errors.Is(err, s3adminext.ErrInvalidScanRoot),
		errors.Is(err, s3adminext.ErrInvalidUserSecretKey),
		errors.Is(err, s3adminext.ErrInvalidUserID),
		errors.Is(err, s3adminext.ErrInvalidUserSecretKeyIRODSPath):
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
	default:
		writePathError(w, err)
	}
}

func errorsIsNotFound(err error) bool {
	return errors.Is(err, irods.ErrNotFound) || strings.Contains(strings.ToLower(err.Error()), "not found")
}
