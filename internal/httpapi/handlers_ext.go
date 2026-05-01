package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"

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

func errorsIsNotFound(err error) bool {
	return errors.Is(err, irods.ErrNotFound) || strings.Contains(strings.ToLower(err.Error()), "not found")
}
