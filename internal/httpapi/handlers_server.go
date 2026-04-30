package httpapi

import (
	"errors"
	"net/http"

	"github.com/michael-conway/irods-go-rest/internal/irods"
)

func (h *Handler) getServerInfo(w http.ResponseWriter, r *http.Request) {
	info, err := h.serverInfo.GetServerInfo(r.Context())
	if err != nil {
		writeServerInfoError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"server_info": info,
	})
}

func writeServerInfoError(w http.ResponseWriter, err error) {
	if errors.Is(err, irods.ErrPermissionDenied) {
		writeError(w, http.StatusForbidden, "permission_denied", err.Error())
		return
	}
	writeError(w, http.StatusInternalServerError, "internal_error", err.Error())
}
