package restservice

import (
	"context"

	"github.com/michael-conway/irods-go-rest/internal/domain"
	"github.com/michael-conway/irods-go-rest/internal/irods"
)

type UserGroupSummaryOptions struct {
	Zone   string
	Prefix string
	Limit  int
}

type UserMembershipSummaryOptions struct {
	Zone   string
	Prefix string
	Type   string
	Limit  int
}

type GroupsForUserOptions struct {
	Zone     string
	UserName string
	Limit    int
}

type PrincipalSearchOptions struct {
	Zone  string
	Query string
	Kinds []string
	Limit int
}

type UsersAndGroupsService interface {
	GetCurrentUserMembership(ctx context.Context, options GroupsForUserOptions) (domain.CurrentUserMembership, error)
	ListGroupSummaries(ctx context.Context, options UserGroupSummaryOptions) ([]domain.UserGroupSummary, error)
	ListUserMembershipSummaries(ctx context.Context, options UserMembershipSummaryOptions) ([]domain.UserMembershipSummary, error)
	ListGroupsForUser(ctx context.Context, options GroupsForUserOptions) ([]domain.UserGroupRef, error)
	SearchPrincipals(ctx context.Context, options PrincipalSearchOptions) ([]domain.PrincipalSearchResult, error)
}

type usersAndGroupsService struct {
	usersAndGroups irods.UsersAndGroupsService
}

func NewUsersAndGroupsService(usersAndGroups irods.UsersAndGroupsService) UsersAndGroupsService {
	return &usersAndGroupsService{usersAndGroups: usersAndGroups}
}

func (s *usersAndGroupsService) GetCurrentUserMembership(ctx context.Context, options GroupsForUserOptions) (domain.CurrentUserMembership, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return domain.CurrentUserMembership{}, err
	}
	return s.usersAndGroups.GetCurrentUserMembership(ctx, irodsRequestContext(requestContext), irods.GroupsForUserOptions{
		Zone:  options.Zone,
		Limit: options.Limit,
	})
}

func (s *usersAndGroupsService) ListGroupSummaries(ctx context.Context, options UserGroupSummaryOptions) ([]domain.UserGroupSummary, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return nil, err
	}
	return s.usersAndGroups.ListGroupSummaries(ctx, irodsRequestContext(requestContext), irods.UserGroupSummaryOptions{
		Zone:   options.Zone,
		Prefix: options.Prefix,
		Limit:  options.Limit,
	})
}

func (s *usersAndGroupsService) ListUserMembershipSummaries(ctx context.Context, options UserMembershipSummaryOptions) ([]domain.UserMembershipSummary, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return nil, err
	}
	return s.usersAndGroups.ListUserMembershipSummaries(ctx, irodsRequestContext(requestContext), irods.UserMembershipSummaryOptions{
		Zone:   options.Zone,
		Prefix: options.Prefix,
		Type:   options.Type,
		Limit:  options.Limit,
	})
}

func (s *usersAndGroupsService) ListGroupsForUser(ctx context.Context, options GroupsForUserOptions) ([]domain.UserGroupRef, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return nil, err
	}
	return s.usersAndGroups.ListGroupsForUser(ctx, irodsRequestContext(requestContext), irods.GroupsForUserOptions{
		Zone:     options.Zone,
		UserName: options.UserName,
		Limit:    options.Limit,
	})
}

func (s *usersAndGroupsService) SearchPrincipals(ctx context.Context, options PrincipalSearchOptions) ([]domain.PrincipalSearchResult, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return nil, err
	}
	return s.usersAndGroups.SearchPrincipals(ctx, irodsRequestContext(requestContext), irods.PrincipalSearchOptions{
		Zone:  options.Zone,
		Query: options.Query,
		Kinds: options.Kinds,
		Limit: options.Limit,
	})
}
