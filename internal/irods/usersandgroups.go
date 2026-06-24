package irods

import (
	"context"
	"log/slog"
	"strings"

	irodstypes "github.com/cyverse/go-irodsclient/irods/types"
	usersandgroupsext "github.com/michael-conway/go-irodsclient-extensions/usersandgroups"
	"github.com/michael-conway/irods-go-rest/internal/config"
	"github.com/michael-conway/irods-go-rest/internal/domain"
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
	GetCurrentUserMembership(ctx context.Context, requestContext *RequestContext, options GroupsForUserOptions) (domain.CurrentUserMembership, error)
	ListGroupSummaries(ctx context.Context, requestContext *RequestContext, options UserGroupSummaryOptions) ([]domain.UserGroupSummary, error)
	ListUserMembershipSummaries(ctx context.Context, requestContext *RequestContext, options UserMembershipSummaryOptions) ([]domain.UserMembershipSummary, error)
	ListGroupsForUser(ctx context.Context, requestContext *RequestContext, options GroupsForUserOptions) ([]domain.UserGroupRef, error)
	SearchPrincipals(ctx context.Context, requestContext *RequestContext, options PrincipalSearchOptions) ([]domain.PrincipalSearchResult, error)
}

type usersAndGroupsService struct {
	cfg              config.RestConfig
	createFileSystem CatalogFileSystemFactory
}

func NewUsersAndGroupsService(cfg config.RestConfig) UsersAndGroupsService {
	return NewUsersAndGroupsServiceWithFactory(cfg, defaultCatalogFileSystemFactory())
}

func NewUsersAndGroupsServiceWithFactory(cfg config.RestConfig, factory CatalogFileSystemFactory) UsersAndGroupsService {
	return &usersAndGroupsService{
		cfg:              cfg,
		createFileSystem: factory,
	}
}

func (s *usersAndGroupsService) GetCurrentUserMembership(ctx context.Context, requestContext *RequestContext, options GroupsForUserOptions) (domain.CurrentUserMembership, error) {
	zone := s.userZone(options.Zone)
	username := strings.TrimSpace(safeUsername(requestContext))
	if username == "" {
		return domain.CurrentUserMembership{}, newInvalidRequestError("authenticated iRODS username is required")
	}
	slog.Debug("usersandgroups GetCurrentUserMembership start", append([]any{"zone", zone, "user", username, "limit", options.Limit}, requestContextLogArgs(requestContext)...)...)

	catalog := &catalogService{
		cfg:              s.cfg,
		createFileSystem: s.createFileSystem,
	}
	filesystem, err := catalog.filesystemForRequest(requestContext, "irods-go-rest-current-user-membership")
	if err != nil {
		return domain.CurrentUserMembership{}, err
	}
	defer filesystem.Release()

	user, err := filesystem.GetUser(username, zone, "")
	if err != nil {
		return domain.CurrentUserMembership{}, normalizeUserError("get current user", username, zone, err)
	}

	service := usersandgroupsext.NewService(filesystem.UsersAndGroupsCatalog(), zone)
	groups, err := service.ListGroupsForUser(ctx, usersandgroupsext.GroupsForUserOptions{
		Zone:     zone,
		UserName: username,
		Limit:    options.Limit,
	})
	if err != nil {
		return domain.CurrentUserMembership{}, normalizeUserGroupError("list current user groups", username, zone, err)
	}

	mappedUser := mapUser(user)
	mappedGroups := mapUserGroupRefs(groups)
	isRodsAdmin := user != nil && user.Type == irodstypes.IRODSUserRodsAdmin
	isGroupAdmin := user != nil && user.Type == irodstypes.IRODSUserGroupAdmin
	return domain.CurrentUserMembership{
		User:                        mappedUser,
		Groups:                      mappedGroups,
		IsRodsAdmin:                 isRodsAdmin,
		IsGroupAdmin:                isGroupAdmin,
		CanAdministerUsersAndGroups: isRodsAdmin || isGroupAdmin,
	}, nil
}

func (s *usersAndGroupsService) ListGroupSummaries(ctx context.Context, requestContext *RequestContext, options UserGroupSummaryOptions) ([]domain.UserGroupSummary, error) {
	zone := s.userZone(options.Zone)
	prefix := strings.TrimSpace(options.Prefix)
	slog.Debug("usersandgroups ListGroupSummaries start", append([]any{"zone", zone, "prefix", prefix, "limit", options.Limit}, requestContextLogArgs(requestContext)...)...)

	service, release, err := s.extensionServiceForRequest(requestContext, zone, "irods-go-rest-list-group-summaries")
	if err != nil {
		return nil, err
	}
	defer release()

	groups, err := service.ListGroupSummaries(ctx, usersandgroupsext.GroupSummaryOptions{
		Zone:   zone,
		Prefix: prefix,
		Limit:  options.Limit,
	})
	if err != nil {
		return nil, normalizeUserGroupError("list group summaries", "", zone, err)
	}

	result := make([]domain.UserGroupSummary, 0, len(groups))
	for _, group := range groups {
		result = append(result, mapUserGroupSummary(group))
	}
	return result, nil
}

func (s *usersAndGroupsService) ListUserMembershipSummaries(ctx context.Context, requestContext *RequestContext, options UserMembershipSummaryOptions) ([]domain.UserMembershipSummary, error) {
	zone := s.userZone(options.Zone)
	prefix := strings.TrimSpace(options.Prefix)
	userType := irodstypes.IRODSUserType(strings.TrimSpace(options.Type))
	if userType == "" {
		userType = irodstypes.IRODSUserRodsUser
	}
	slog.Debug("usersandgroups ListUserMembershipSummaries start", append([]any{"zone", zone, "prefix", prefix, "type", userType, "limit", options.Limit}, requestContextLogArgs(requestContext)...)...)

	service, release, err := s.extensionServiceForRequest(requestContext, zone, "irods-go-rest-list-user-membership-summaries")
	if err != nil {
		return nil, err
	}
	defer release()

	users, err := service.ListUserMembershipSummaries(ctx, usersandgroupsext.UserMembershipSummaryOptions{
		Zone:   zone,
		Prefix: prefix,
		Type:   userType,
		Limit:  options.Limit,
	})
	if err != nil {
		return nil, normalizeUserError("list user membership summaries", "", zone, err)
	}

	result := make([]domain.UserMembershipSummary, 0, len(users))
	for _, user := range users {
		result = append(result, mapUserMembershipSummary(user))
	}
	return result, nil
}

func (s *usersAndGroupsService) ListGroupsForUser(ctx context.Context, requestContext *RequestContext, options GroupsForUserOptions) ([]domain.UserGroupRef, error) {
	zone := s.userZone(options.Zone)
	username := strings.TrimSpace(options.UserName)
	service, release, err := s.extensionServiceForRequest(requestContext, zone, "irods-go-rest-list-groups-for-user")
	if err != nil {
		return nil, err
	}
	defer release()

	groups, err := service.ListGroupsForUser(ctx, usersandgroupsext.GroupsForUserOptions{
		Zone:     zone,
		UserName: username,
		Limit:    options.Limit,
	})
	if err != nil {
		return nil, normalizeUserGroupError("list groups for user", username, zone, err)
	}

	result := make([]domain.UserGroupRef, 0, len(groups))
	for _, group := range groups {
		result = append(result, mapUserGroupRef(group))
	}
	return result, nil
}

func (s *usersAndGroupsService) SearchPrincipals(ctx context.Context, requestContext *RequestContext, options PrincipalSearchOptions) ([]domain.PrincipalSearchResult, error) {
	zone := s.userZone(options.Zone)
	query := strings.TrimSpace(options.Query)
	service, release, err := s.extensionServiceForRequest(requestContext, zone, "irods-go-rest-search-principals")
	if err != nil {
		return nil, err
	}
	defer release()

	results, err := service.SearchPrincipals(ctx, usersandgroupsext.PrincipalSearchOptions{
		Zone:  zone,
		Query: query,
		Kinds: principalKinds(options.Kinds),
		Limit: options.Limit,
	})
	if err != nil {
		return nil, normalizeUserError("search principals", "", zone, err)
	}

	mapped := make([]domain.PrincipalSearchResult, 0, len(results))
	for _, result := range results {
		mapped = append(mapped, mapPrincipalSearchResult(result))
	}
	return mapped, nil
}

func (s *usersAndGroupsService) extensionServiceForRequest(requestContext *RequestContext, zone string, applicationName string) (*usersandgroupsext.Service, func(), error) {
	catalog := &catalogService{
		cfg:              s.cfg,
		createFileSystem: s.createFileSystem,
	}
	filesystem, err := catalog.filesystemForRequest(requestContext, applicationName)
	if err != nil {
		return nil, nil, err
	}
	return usersandgroupsext.NewService(filesystem.UsersAndGroupsCatalog(), zone), filesystem.Release, nil
}

func (s *usersAndGroupsService) userZone(zone string) string {
	if trimmed := strings.TrimSpace(zone); trimmed != "" {
		return trimmed
	}
	return strings.TrimSpace(s.cfg.IrodsZone)
}

func principalKinds(kinds []string) []usersandgroupsext.PrincipalKind {
	result := make([]usersandgroupsext.PrincipalKind, 0, len(kinds))
	for _, kind := range kinds {
		switch strings.TrimSpace(kind) {
		case string(usersandgroupsext.PrincipalKindUser):
			result = append(result, usersandgroupsext.PrincipalKindUser)
		case string(usersandgroupsext.PrincipalKindGroup):
			result = append(result, usersandgroupsext.PrincipalKindGroup)
		}
	}
	return result
}

func mapUserGroupSummary(group usersandgroupsext.GroupSummary) domain.UserGroupSummary {
	return domain.UserGroupSummary{
		ID:          group.ID,
		Name:        group.Name,
		Zone:        group.Zone,
		Type:        string(group.Type),
		MemberCount: group.MemberCount,
	}
}

func mapUserMembershipSummary(user usersandgroupsext.UserMembershipSummary) domain.UserMembershipSummary {
	return domain.UserMembershipSummary{
		ID:     user.ID,
		Name:   user.Name,
		Zone:   user.Zone,
		Type:   string(user.Type),
		Groups: mapUserGroupRefs(user.Groups),
	}
}

func mapUserGroupRefs(groups []usersandgroupsext.GroupRef) []domain.UserGroupRef {
	if len(groups) == 0 {
		return []domain.UserGroupRef{}
	}
	result := make([]domain.UserGroupRef, 0, len(groups))
	for _, group := range groups {
		result = append(result, mapUserGroupRef(group))
	}
	return result
}

func mapUserGroupRef(group usersandgroupsext.GroupRef) domain.UserGroupRef {
	return domain.UserGroupRef{
		ID:   group.ID,
		Name: group.Name,
		Zone: group.Zone,
		Type: string(group.Type),
	}
}

func mapPrincipalSearchResult(result usersandgroupsext.PrincipalSearchResult) domain.PrincipalSearchResult {
	return domain.PrincipalSearchResult{
		ID:    result.ID,
		Name:  result.Name,
		Zone:  result.Zone,
		Type:  string(result.Type),
		Kind:  string(result.Kind),
		Score: result.Score,
	}
}
