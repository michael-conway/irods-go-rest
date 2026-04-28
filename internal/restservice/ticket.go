package restservice

import (
	"context"

	"github.com/michael-conway/irods-go-rest/internal/domain"
	"github.com/michael-conway/irods-go-rest/internal/irods"
)

type TicketService interface {
	CreateAnonymousTicket(ctx context.Context, absolutePath string, options irods.TicketCreateOptions) (domain.Ticket, error)
	ListTickets(ctx context.Context) ([]domain.Ticket, error)
	GetTicket(ctx context.Context, ticketName string) (domain.Ticket, error)
	UpdateTicket(ctx context.Context, ticketName string, options irods.TicketUpdateOptions) (domain.Ticket, error)
	DeleteTicket(ctx context.Context, ticketName string) error
}

type ticketService struct {
	tickets irods.TicketService
}

func NewTicketService(tickets irods.TicketService) TicketService {
	return &ticketService{tickets: tickets}
}

func (s *ticketService) CreateAnonymousTicket(ctx context.Context, absolutePath string, options irods.TicketCreateOptions) (domain.Ticket, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return domain.Ticket{}, err
	}

	return s.tickets.CreateAnonymousTicket(ctx, irodsRequestContext(requestContext), absolutePath, options)
}

func (s *ticketService) ListTickets(ctx context.Context) ([]domain.Ticket, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return nil, err
	}

	return s.tickets.ListTickets(ctx, irodsRequestContext(requestContext))
}

func (s *ticketService) GetTicket(ctx context.Context, ticketName string) (domain.Ticket, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return domain.Ticket{}, err
	}

	return s.tickets.GetTicket(ctx, irodsRequestContext(requestContext), ticketName)
}

func (s *ticketService) UpdateTicket(ctx context.Context, ticketName string, options irods.TicketUpdateOptions) (domain.Ticket, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return domain.Ticket{}, err
	}

	return s.tickets.UpdateTicket(ctx, irodsRequestContext(requestContext), ticketName, options)
}

func (s *ticketService) DeleteTicket(ctx context.Context, ticketName string) error {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return err
	}

	return s.tickets.DeleteTicket(ctx, irodsRequestContext(requestContext), ticketName)
}
