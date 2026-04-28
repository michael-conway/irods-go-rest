package irods

import (
	"context"
	"fmt"
	"strings"
	"time"

	irodsfs "github.com/cyverse/go-irodsclient/fs"
	irodscommon "github.com/cyverse/go-irodsclient/irods/common"
	irodstypes "github.com/cyverse/go-irodsclient/irods/types"
	extensiontickets "github.com/michael-conway/go-irodsclient-extensions/tickets"
	"github.com/michael-conway/irods-go-rest/internal/config"
	"github.com/michael-conway/irods-go-rest/internal/domain"
)

type TicketCreateOptions struct {
	MaximumUses     int64
	LifetimeMinutes int
}

type TicketUpdateOptions struct {
	MaximumUses     *int64
	LifetimeMinutes *int
}

type TicketService interface {
	CreateAnonymousTicket(ctx context.Context, requestContext *RequestContext, absolutePath string, options TicketCreateOptions) (domain.Ticket, error)
	ListTickets(ctx context.Context, requestContext *RequestContext) ([]domain.Ticket, error)
	GetTicket(ctx context.Context, requestContext *RequestContext, ticketName string) (domain.Ticket, error)
	UpdateTicket(ctx context.Context, requestContext *RequestContext, ticketName string, options TicketUpdateOptions) (domain.Ticket, error)
	DeleteTicket(ctx context.Context, requestContext *RequestContext, ticketName string) error
}

var ticketNow = time.Now
var ticketVisibilityTimeout = 2 * time.Second
var ticketVisibilityPollInterval = 100 * time.Millisecond

func NewTicketService(cfg config.RestConfig) TicketService {
	return NewTicketServiceWithFactory(cfg, func(account *irodstypes.IRODSAccount, applicationName string) (CatalogFileSystem, error) {
		filesystem, err := irodsfs.NewFileSystemWithDefault(account, applicationName)
		if err != nil {
			return nil, err
		}

		return &catalogFileSystemAdapter{filesystem: filesystem}, nil
	})
}

func NewTicketServiceWithFactory(cfg config.RestConfig, factory CatalogFileSystemFactory) TicketService {
	return &catalogService{
		cfg:              cfg,
		createFileSystem: factory,
	}
}

func (s *catalogService) CreateAnonymousTicket(_ context.Context, requestContext *RequestContext, absolutePath string, options TicketCreateOptions) (domain.Ticket, error) {
	absolutePath = strings.TrimSpace(absolutePath)
	if absolutePath == "" {
		return domain.Ticket{}, fmt.Errorf("%w: path %q", ErrNotFound, absolutePath)
	}
	if options.MaximumUses < 0 {
		return domain.Ticket{}, fmt.Errorf("maximum uses must be zero or greater")
	}
	if options.LifetimeMinutes < 0 {
		return domain.Ticket{}, fmt.Errorf("ticket lifetime minutes must be zero or greater")
	}

	filesystem, err := s.filesystemForRequest(requestContext, "irods-go-rest-create-ticket")
	if err != nil {
		logIRODSError("ticket CreateAnonymousTicket filesystem setup failed", err, "path", absolutePath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.Ticket{}, err
	}
	defer filesystem.Release()

	if _, err := filesystem.Stat(absolutePath); err != nil {
		logIRODSError("ticket CreateAnonymousTicket stat failed", err, "path", absolutePath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.Ticket{}, normalizePathAccessError("stat path", absolutePath, err)
	}

	ticketName, _, err := extensiontickets.CreateAnonymousDataObjectBearerToken(filesystem, absolutePath, options.MaximumUses, options.LifetimeMinutes)
	if err != nil {
		logIRODSError("ticket CreateAnonymousTicket create failed", err, "path", absolutePath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.Ticket{}, normalizeTicketAccessError("create ticket", "", err)
	}

	ticket, err := waitForCreatedTicket(filesystem, ticketName, absolutePath)
	if err != nil {
		logIRODSError("ticket CreateAnonymousTicket fetch failed", err, "ticket_name", ticketName, "path", absolutePath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.Ticket{}, normalizeTicketAccessError("get ticket", ticketName, err)
	}

	return domainTicket(ticket), nil
}

func (s *catalogService) ListTickets(_ context.Context, requestContext *RequestContext) ([]domain.Ticket, error) {
	filesystem, err := s.filesystemForRequest(requestContext, "irods-go-rest-list-tickets")
	if err != nil {
		logIRODSError("ticket ListTickets filesystem setup failed", err, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return nil, err
	}
	defer filesystem.Release()

	tickets, err := filesystem.ListTickets()
	if err != nil {
		logIRODSError("ticket ListTickets failed", err, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return nil, normalizeTicketAccessError("list tickets", "", err)
	}

	return filterOwnedTickets(requestContext, tickets), nil
}

func (s *catalogService) GetTicket(_ context.Context, requestContext *RequestContext, ticketName string) (domain.Ticket, error) {
	ticketName = strings.TrimSpace(ticketName)
	if ticketName == "" {
		return domain.Ticket{}, fmt.Errorf("%w: ticket %q", ErrNotFound, ticketName)
	}

	filesystem, err := s.filesystemForRequest(requestContext, "irods-go-rest-get-ticket")
	if err != nil {
		logIRODSError("ticket GetTicket filesystem setup failed", err, "ticket_name", ticketName, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.Ticket{}, err
	}
	defer filesystem.Release()

	ticket, err := resolveOwnedTicket(filesystem, requestContext, ticketName)
	if err != nil {
		logIRODSError("ticket GetTicket failed", err, "ticket_name", ticketName, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.Ticket{}, normalizeTicketAccessError("get ticket", ticketName, err)
	}

	return domainTicket(ticket), nil
}

func (s *catalogService) UpdateTicket(ctx context.Context, requestContext *RequestContext, ticketName string, options TicketUpdateOptions) (domain.Ticket, error) {
	ticketName = strings.TrimSpace(ticketName)
	if ticketName == "" {
		return domain.Ticket{}, fmt.Errorf("%w: ticket %q", ErrNotFound, ticketName)
	}
	if options.MaximumUses != nil && *options.MaximumUses < 0 {
		return domain.Ticket{}, fmt.Errorf("maximum uses must be zero or greater")
	}
	if options.LifetimeMinutes != nil && *options.LifetimeMinutes < 0 {
		return domain.Ticket{}, fmt.Errorf("ticket lifetime minutes must be zero or greater")
	}

	filesystem, err := s.filesystemForRequest(requestContext, "irods-go-rest-update-ticket")
	if err != nil {
		logIRODSError("ticket UpdateTicket filesystem setup failed", err, "ticket_name", ticketName, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.Ticket{}, err
	}
	defer filesystem.Release()

	current, err := s.GetTicket(ctx, requestContext, ticketName)
	if err != nil {
		return domain.Ticket{}, err
	}
	_ = current

	if options.MaximumUses != nil {
		if *options.MaximumUses == 0 {
			if err := filesystem.ClearTicketUseLimit(ticketName); err != nil {
				logIRODSError("ticket UpdateTicket clear use limit failed", err, "ticket_name", ticketName, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
				return domain.Ticket{}, normalizeTicketAccessError("clear ticket use limit", ticketName, err)
			}
		} else {
			if err := filesystem.ModifyTicketUseLimit(ticketName, *options.MaximumUses); err != nil {
				logIRODSError("ticket UpdateTicket set use limit failed", err, "ticket_name", ticketName, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
				return domain.Ticket{}, normalizeTicketAccessError("modify ticket use limit", ticketName, err)
			}
		}
	}

	if options.LifetimeMinutes != nil {
		if *options.LifetimeMinutes == 0 {
			if err := filesystem.ClearTicketExpirationTime(ticketName); err != nil {
				logIRODSError("ticket UpdateTicket clear expiration failed", err, "ticket_name", ticketName, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
				return domain.Ticket{}, normalizeTicketAccessError("clear ticket expiration", ticketName, err)
			}
		} else {
			expirationTime := ticketNow().Add(time.Duration(*options.LifetimeMinutes) * time.Minute)
			if err := filesystem.ModifyTicketExpirationTime(ticketName, expirationTime); err != nil {
				logIRODSError("ticket UpdateTicket set expiration failed", err, "ticket_name", ticketName, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
				return domain.Ticket{}, normalizeTicketAccessError("modify ticket expiration", ticketName, err)
			}
		}
	}

	return s.GetTicket(ctx, requestContext, ticketName)
}

func (s *catalogService) DeleteTicket(_ context.Context, requestContext *RequestContext, ticketName string) error {
	ticketName = strings.TrimSpace(ticketName)
	if ticketName == "" {
		return fmt.Errorf("%w: ticket %q", ErrNotFound, ticketName)
	}

	filesystem, err := s.filesystemForRequest(requestContext, "irods-go-rest-delete-ticket")
	if err != nil {
		logIRODSError("ticket DeleteTicket filesystem setup failed", err, "ticket_name", ticketName, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return err
	}
	defer filesystem.Release()

	ticket, err := resolveOwnedTicket(filesystem, requestContext, ticketName)
	if err != nil {
		logIRODSError("ticket DeleteTicket fetch failed", err, "ticket_name", ticketName, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return normalizeTicketAccessError("get ticket", ticketName, err)
	}
	_ = ticket

	if err := filesystem.DeleteTicket(ticketName); err != nil {
		logIRODSError("ticket DeleteTicket failed", err, "ticket_name", ticketName, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return normalizeTicketAccessError("delete ticket", ticketName, err)
	}

	return nil
}

func domainTicket(ticket *irodstypes.IRODSTicket) domain.Ticket {
	if ticket == nil {
		return domain.Ticket{}
	}

	result := domain.Ticket{
		Name:           strings.TrimSpace(ticket.Name),
		BearerToken:    extensiontickets.FormatBearerToken(ticket.Name),
		Type:           strings.TrimSpace(string(ticket.Type)),
		Owner:          strings.TrimSpace(ticket.Owner),
		OwnerZone:      strings.TrimSpace(ticket.OwnerZone),
		ObjectType:     strings.TrimSpace(string(ticket.ObjectType)),
		Path:           strings.TrimSpace(ticket.Path),
		UsesLimit:      ticket.UsesLimit,
		UsesCount:      ticket.UsesCount,
		WriteFileLimit: ticket.WriteFileLimit,
		WriteFileCount: ticket.WriteFileCount,
		WriteByteLimit: ticket.WriteByteLimit,
		WriteByteCount: ticket.WriteByteCount,
	}

	if !ticket.ExpirationTime.IsZero() {
		expiresAt := ticket.ExpirationTime.UTC()
		result.ExpirationTime = &expiresAt
	}

	return result
}

func filterOwnedTickets(requestContext *RequestContext, tickets []*irodstypes.IRODSTicket) []domain.Ticket {
	if len(tickets) == 0 {
		return nil
	}

	results := make([]domain.Ticket, 0, len(tickets))
	for _, ticket := range tickets {
		if ticket == nil || !ticketOwnedByRequest(requestContext, ticket) {
			continue
		}

		results = append(results, domainTicket(ticket))
	}

	if len(results) == 0 {
		return nil
	}

	return results
}

func ticketOwnedByRequest(requestContext *RequestContext, ticket *irodstypes.IRODSTicket) bool {
	if requestContext == nil || ticket == nil {
		return false
	}

	username := strings.TrimSpace(requestContext.Username)
	if username == "" {
		return false
	}

	return strings.EqualFold(strings.TrimSpace(ticket.Owner), username)
}

func waitForCreatedTicket(filesystem CatalogFileSystem, ticketName string, objectPath string) (*irodstypes.IRODSTicket, error) {
	ticketName = strings.TrimSpace(ticketName)
	objectPath = strings.TrimSpace(objectPath)
	if ticketName == "" {
		return nil, irodstypes.NewTicketNotFoundError(ticketName)
	}

	if ticket, err := filesystem.GetTicket(ticketName); err == nil && ticket != nil {
		return ticket, nil
	}

	deadline := time.Now().Add(ticketVisibilityTimeout)
	var lastErr error
	for time.Now().Before(deadline) {
		tickets, err := filesystem.ListTickets()
		if err != nil {
			lastErr = err
		} else {
			for _, ticket := range tickets {
				if ticket == nil {
					continue
				}
				if strings.TrimSpace(ticket.Name) == ticketName && strings.TrimSpace(ticket.Path) == objectPath {
					return ticket, nil
				}
			}
		}

		time.Sleep(ticketVisibilityPollInterval)
	}

	if lastErr != nil {
		return nil, lastErr
	}

	return nil, irodstypes.NewTicketNotFoundError(ticketName)
}

func resolveOwnedTicket(filesystem CatalogFileSystem, requestContext *RequestContext, ticketName string) (*irodstypes.IRODSTicket, error) {
	ticketName = strings.TrimSpace(ticketName)
	if ticketName == "" {
		return nil, irodstypes.NewTicketNotFoundError(ticketName)
	}

	if ticket, err := filesystem.GetTicket(ticketName); err == nil && ticket != nil {
		if ticketOwnedByRequest(requestContext, ticket) {
			return ticket, nil
		}
		return nil, irodstypes.NewTicketNotFoundError(ticketName)
	}

	tickets, err := filesystem.ListTickets()
	if err != nil {
		return nil, err
	}

	for _, ticket := range tickets {
		if ticket == nil {
			continue
		}
		if strings.TrimSpace(ticket.Name) == ticketName && ticketOwnedByRequest(requestContext, ticket) {
			return ticket, nil
		}
	}

	return nil, irodstypes.NewTicketNotFoundError(ticketName)
}

func normalizeTicketAccessError(operation string, ticketName string, err error) error {
	if err == nil {
		return nil
	}

	if strings.TrimSpace(ticketName) != "" && irodstypes.IsTicketNotFoundError(err) {
		return fmt.Errorf("%w: ticket %q", ErrNotFound, ticketName)
	}

	switch irodstypes.GetIRODSErrorCode(err) {
	case irodscommon.CAT_NO_ACCESS_PERMISSION, irodscommon.SYS_NO_API_PRIV:
		if ticketName == "" {
			return fmt.Errorf("%w: tickets", ErrPermissionDenied)
		}
		return fmt.Errorf("%w: ticket %q", ErrPermissionDenied, ticketName)
	}

	message := strings.ToLower(err.Error())
	if strings.Contains(message, "ticket") && strings.Contains(message, "not found") {
		return fmt.Errorf("%w: ticket %q", ErrNotFound, ticketName)
	}
	if strings.Contains(message, "no access permission") || strings.Contains(message, "permission denied") {
		if ticketName == "" {
			return fmt.Errorf("%w: tickets", ErrPermissionDenied)
		}
		return fmt.Errorf("%w: ticket %q", ErrPermissionDenied, ticketName)
	}

	if ticketName == "" {
		return fmt.Errorf("%s: %w", operation, err)
	}
	return fmt.Errorf("%s %q: %w", operation, ticketName, err)
}
