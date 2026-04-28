package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/michael-conway/irods-go-rest/internal/domain"
	"github.com/michael-conway/irods-go-rest/internal/irods"
)

func (h *Handler) postPathTicket(w http.ResponseWriter, r *http.Request) {
	objectPath := queryIRODSPath(r)
	if objectPath == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "irods_path query parameter is required")
		return
	}

	options, err := decodeTicketCreateOptions(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	ticket, err := h.tickets.CreateAnonymousTicket(r.Context(), objectPath, options)
	if err != nil {
		writeTicketError(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"ticket": ticketResponse(ticket),
	})
}

func (h *Handler) getTickets(w http.ResponseWriter, r *http.Request) {
	tickets, err := h.tickets.ListTickets(r.Context())
	if err != nil {
		writeTicketError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"tickets": ticketResponseList(tickets),
		"count":   len(tickets),
		"links": map[string]any{
			"self": domain.ActionLink{
				Href:   "/api/v1/ticket",
				Method: http.MethodGet,
			},
			"create": domain.ActionLink{
				Href:   "/api/v1/ticket",
				Method: http.MethodPost,
			},
		},
	})
}

func (h *Handler) postTicket(w http.ResponseWriter, r *http.Request) {
	var request struct {
		IRODSPath       string `json:"irods_path"`
		MaximumUses     *int64 `json:"maximum_uses"`
		LifetimeMinutes *int   `json:"lifetime_minutes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must be valid JSON")
		return
	}

	request.IRODSPath = strings.TrimSpace(request.IRODSPath)
	if request.IRODSPath == "" {
		writeValidationError(w, http.StatusBadRequest, "invalid_request", "ticket request validation failed", map[string]string{
			"irods_path": "irods_path is required",
		})
		return
	}

	options, err := validateTicketCreateOptions(request.MaximumUses, request.LifetimeMinutes)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	ticket, err := h.tickets.CreateAnonymousTicket(r.Context(), request.IRODSPath, options)
	if err != nil {
		writeTicketError(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"ticket": ticketResponse(ticket),
	})
}

func (h *Handler) getTicket(w http.ResponseWriter, r *http.Request) {
	ticketName := pathValue(r, "ticket_name")
	if ticketName == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "ticket_name path parameter is required")
		return
	}

	ticket, err := h.tickets.GetTicket(r.Context(), ticketName)
	if err != nil {
		writeTicketError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"ticket": ticketResponse(ticket),
	})
}

func (h *Handler) patchTicket(w http.ResponseWriter, r *http.Request) {
	ticketName := pathValue(r, "ticket_name")
	if ticketName == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "ticket_name path parameter is required")
		return
	}

	var request struct {
		MaximumUses     *int64 `json:"maximum_uses"`
		LifetimeMinutes *int   `json:"lifetime_minutes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must be valid JSON")
		return
	}
	if request.MaximumUses == nil && request.LifetimeMinutes == nil {
		writeValidationError(w, http.StatusBadRequest, "invalid_request", "ticket update validation failed", map[string]string{
			"maximum_uses":     "maximum_uses or lifetime_minutes is required",
			"lifetime_minutes": "maximum_uses or lifetime_minutes is required",
		})
		return
	}
	if request.MaximumUses != nil && *request.MaximumUses < 0 {
		writeValidationError(w, http.StatusBadRequest, "invalid_request", "ticket update validation failed", map[string]string{
			"maximum_uses": "maximum_uses must be zero or greater",
		})
		return
	}
	if request.LifetimeMinutes != nil && *request.LifetimeMinutes < 0 {
		writeValidationError(w, http.StatusBadRequest, "invalid_request", "ticket update validation failed", map[string]string{
			"lifetime_minutes": "lifetime_minutes must be zero or greater",
		})
		return
	}

	ticket, err := h.tickets.UpdateTicket(r.Context(), ticketName, irods.TicketUpdateOptions{
		MaximumUses:     request.MaximumUses,
		LifetimeMinutes: request.LifetimeMinutes,
	})
	if err != nil {
		writeTicketError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"ticket": ticketResponse(ticket),
	})
}

func (h *Handler) deleteTicket(w http.ResponseWriter, r *http.Request) {
	ticketName := pathValue(r, "ticket_name")
	if ticketName == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "ticket_name path parameter is required")
		return
	}

	if err := h.tickets.DeleteTicket(r.Context(), ticketName); err != nil {
		writeTicketError(w, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func decodeTicketCreateOptions(r *http.Request) (irods.TicketCreateOptions, error) {
	var request struct {
		MaximumUses     *int64 `json:"maximum_uses"`
		LifetimeMinutes *int   `json:"lifetime_minutes"`
	}

	if r.Body != nil {
		decoder := json.NewDecoder(r.Body)
		if err := decoder.Decode(&request); err != nil && !errors.Is(err, io.EOF) {
			return irods.TicketCreateOptions{}, err
		}
	}

	return validateTicketCreateOptions(request.MaximumUses, request.LifetimeMinutes)
}

func validateTicketCreateOptions(maximumUses *int64, lifetimeMinutes *int) (irods.TicketCreateOptions, error) {
	options := irods.TicketCreateOptions{}
	if maximumUses != nil {
		if *maximumUses < 0 {
			return irods.TicketCreateOptions{}, errors.New("maximum_uses must be zero or greater")
		}
		options.MaximumUses = *maximumUses
	}
	if lifetimeMinutes != nil {
		if *lifetimeMinutes < 0 {
			return irods.TicketCreateOptions{}, errors.New("lifetime_minutes must be zero or greater")
		}
		options.LifetimeMinutes = *lifetimeMinutes
	}

	return options, nil
}

func writeTicketError(w http.ResponseWriter, err error) {
	if errors.Is(err, irods.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not_found", err.Error())
		return
	}
	if errors.Is(err, irods.ErrPermissionDenied) {
		writeError(w, http.StatusForbidden, "permission_denied", err.Error())
		return
	}

	lower := strings.ToLower(err.Error())
	if strings.Contains(lower, "must be zero or greater") || strings.Contains(lower, "required") || strings.Contains(lower, "missing authenticated principal") {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	writeError(w, http.StatusInternalServerError, "internal_error", err.Error())
}

func ticketResponseList(tickets []domain.Ticket) []domain.Ticket {
	if len(tickets) == 0 {
		return nil
	}

	results := make([]domain.Ticket, 0, len(tickets))
	for _, ticket := range tickets {
		results = append(results, ticketResponse(ticket))
	}
	return results
}

func ticketResponse(ticket domain.Ticket) domain.Ticket {
	ticket.Links = ticketLinks(ticket)
	return ticket
}

func ticketLinks(ticket domain.Ticket) *domain.TicketLinks {
	ticketName := strings.TrimSpace(ticket.Name)
	if ticketName == "" {
		return nil
	}

	selfHref := "/api/v1/ticket/" + url.PathEscape(ticketName)
	result := &domain.TicketLinks{
		Self: &domain.ActionLink{
			Href:   selfHref,
			Method: http.MethodGet,
		},
		Update: &domain.ActionLink{
			Href:   selfHref,
			Method: http.MethodPatch,
		},
		Delete: &domain.ActionLink{
			Href:   selfHref,
			Method: http.MethodDelete,
		},
	}

	if ticket.Path != "" {
		result.Path = &domain.ActionLink{
			Href:   "/api/v1/path?irods_path=" + url.QueryEscape(ticket.Path),
			Method: http.MethodGet,
		}
		result.Download = &domain.ActionLink{
			Href:   "/api/v1/path/contents?irods_path=" + url.QueryEscape(ticket.Path) + "&ticket_id=" + url.QueryEscape(ticket.Name),
			Method: http.MethodGet,
		}
	}

	return result
}
