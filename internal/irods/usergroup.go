package irods

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	irodsfs "github.com/cyverse/go-irodsclient/fs"
	irodscommon "github.com/cyverse/go-irodsclient/irods/common"
	irodstypes "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/michael-conway/irods-go-rest/internal/config"
	"github.com/michael-conway/irods-go-rest/internal/domain"
)

type UserGroupListOptions struct {
	Zone   string
	Prefix string
}

type UserGroupMutationOptions struct {
	Reconcile bool
}

type UserGroupService interface {
	ListUserGroups(ctx context.Context, requestContext *RequestContext, options UserGroupListOptions) ([]domain.UserGroup, error)
	GetUserGroup(ctx context.Context, requestContext *RequestContext, groupName string, zone string) (domain.UserGroup, error)
	CreateUserGroup(ctx context.Context, requestContext *RequestContext, groupName string, zone string, options UserGroupMutationOptions) (domain.UserGroup, error)
	DeleteUserGroup(ctx context.Context, requestContext *RequestContext, groupName string, zone string, options UserGroupMutationOptions) error
	AddUserToGroup(ctx context.Context, requestContext *RequestContext, groupName string, username string, zone string, options UserGroupMutationOptions) (domain.UserGroup, error)
	RemoveUserFromGroup(ctx context.Context, requestContext *RequestContext, groupName string, username string, zone string, options UserGroupMutationOptions) (domain.UserGroup, error)
}

type userGroupService struct {
	cfg              config.RestConfig
	createFileSystem CatalogFileSystemFactory
}

const irodsUserNotInGroupErrorCode irodscommon.ErrorCode = -1830000

func NewUserGroupService(cfg config.RestConfig) UserGroupService {
	return NewUserGroupServiceWithFactory(cfg, func(account *irodstypes.IRODSAccount, applicationName string) (CatalogFileSystem, error) {
		filesystem, err := irodsfs.NewFileSystemWithDefault(account, applicationName)
		if err != nil {
			return nil, err
		}
		return &catalogFileSystemAdapter{filesystem: filesystem}, nil
	})
}

func NewUserGroupServiceWithFactory(cfg config.RestConfig, factory CatalogFileSystemFactory) UserGroupService {
	return &userGroupService{
		cfg:              cfg,
		createFileSystem: factory,
	}
}

func (s *userGroupService) ListUserGroups(_ context.Context, requestContext *RequestContext, options UserGroupListOptions) ([]domain.UserGroup, error) {
	zone := s.userZone(options.Zone)
	prefix := strings.TrimSpace(options.Prefix)

	slog.Debug("usergroup ListUserGroups start", "zone", zone, "prefix", prefix, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))

	filesystem, err := s.filesystemForRequest(requestContext, "irods-go-rest-list-user-groups")
	if err != nil {
		logIRODSError("usergroup ListUserGroups filesystem setup failed", err, "zone", zone, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return nil, err
	}
	defer filesystem.Release()

	groups, err := filesystem.ListUsers(zone, irodstypes.IRODSUserRodsGroup)
	if err != nil {
		logIRODSError("usergroup ListUserGroups failed", err, "zone", zone, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return nil, normalizeUserGroupError("list groups", "", zone, err)
	}

	result := make([]domain.UserGroup, 0, len(groups))
	for _, group := range groups {
		if group == nil {
			continue
		}

		name := strings.TrimSpace(group.Name)
		if prefix != "" && !strings.HasPrefix(name, prefix) {
			continue
		}

		result = append(result, mapUserGroup(group))
	}

	sortUserGroups(result)
	return result, nil
}

func (s *userGroupService) GetUserGroup(_ context.Context, requestContext *RequestContext, groupName string, zone string) (domain.UserGroup, error) {
	groupName = strings.TrimSpace(groupName)
	zone = s.userZone(zone)
	if groupName == "" {
		return domain.UserGroup{}, fmt.Errorf("%w: group %q", ErrNotFound, groupName)
	}

	slog.Debug("usergroup GetUserGroup start", "group", groupName, "zone", zone, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))

	filesystem, err := s.filesystemForRequest(requestContext, "irods-go-rest-get-user-group")
	if err != nil {
		logIRODSError("usergroup GetUserGroup filesystem setup failed", err, "group", groupName, "zone", zone, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.UserGroup{}, err
	}
	defer filesystem.Release()

	group, err := s.getGroup(filesystem, groupName, zone)
	if err != nil {
		return domain.UserGroup{}, err
	}

	return s.groupWithMembers(filesystem, group)
}

func (s *userGroupService) CreateUserGroup(_ context.Context, requestContext *RequestContext, groupName string, zone string, options UserGroupMutationOptions) (domain.UserGroup, error) {
	groupName = strings.TrimSpace(groupName)
	zone = s.userZone(zone)
	if groupName == "" {
		return domain.UserGroup{}, fmt.Errorf("%w: group %q", ErrNotFound, groupName)
	}

	slog.Debug("usergroup CreateUserGroup start", append([]any{"group", groupName, "zone", zone, "reconcile", options.Reconcile}, requestContextLogArgs(requestContext)...)...)

	filesystem, err := s.filesystemForRequest(requestContext, "irods-go-rest-create-user-group")
	if err != nil {
		logIRODSError("usergroup CreateUserGroup filesystem setup failed", err, append([]any{"group", groupName, "zone", zone, "reconcile", options.Reconcile}, requestContextLogArgs(requestContext)...)...)
		return domain.UserGroup{}, err
	}
	defer filesystem.Release()

	if err := s.requireManageUserGroupsPermission(filesystem, requestContext); err != nil {
		return domain.UserGroup{}, err
	}

	if _, err := filesystem.CreateUserGroup(groupName, zone); err != nil {
		normalizedErr := normalizeUserGroupError("create group", groupName, zone, err)
		if options.Reconcile && errors.Is(normalizedErr, ErrConflict) {
			group, groupErr := s.getGroup(filesystem, groupName, zone)
			if groupErr != nil {
				return domain.UserGroup{}, groupErr
			}
			slog.Info("usergroup CreateUserGroup reconciled existing group", append([]any{"group", groupName, "zone", zone, "outcome", "already_exists"}, requestContextLogArgs(requestContext)...)...)
			return s.groupWithMembers(filesystem, group)
		}
		logIRODSError("usergroup CreateUserGroup failed", normalizedErr, append([]any{"group", groupName, "zone", zone, "reconcile", options.Reconcile}, requestContextLogArgs(requestContext)...)...)
		return domain.UserGroup{}, normalizedErr
	}

	group, err := s.getGroup(filesystem, groupName, zone)
	if err != nil {
		return domain.UserGroup{}, err
	}

	group, err = s.groupWithMembers(filesystem, group)
	if err != nil {
		return domain.UserGroup{}, err
	}
	slog.Info("usergroup CreateUserGroup completed", append([]any{"group", groupName, "zone", zone, "outcome", "created"}, requestContextLogArgs(requestContext)...)...)
	return group, nil
}

func (s *userGroupService) DeleteUserGroup(_ context.Context, requestContext *RequestContext, groupName string, zone string, options UserGroupMutationOptions) error {
	groupName = strings.TrimSpace(groupName)
	zone = s.userZone(zone)
	if groupName == "" {
		return fmt.Errorf("%w: group %q", ErrNotFound, groupName)
	}

	slog.Debug("usergroup DeleteUserGroup start", append([]any{"group", groupName, "zone", zone, "reconcile", options.Reconcile}, requestContextLogArgs(requestContext)...)...)

	filesystem, err := s.filesystemForRequest(requestContext, "irods-go-rest-delete-user-group")
	if err != nil {
		logIRODSError("usergroup DeleteUserGroup filesystem setup failed", err, append([]any{"group", groupName, "zone", zone, "reconcile", options.Reconcile}, requestContextLogArgs(requestContext)...)...)
		return err
	}
	defer filesystem.Release()

	if err := s.requireManageUserGroupsPermission(filesystem, requestContext); err != nil {
		return err
	}

	group, err := s.getGroup(filesystem, groupName, zone)
	if err != nil {
		if options.Reconcile && errors.Is(err, ErrNotFound) {
			slog.Info("usergroup DeleteUserGroup reconciled absent group", append([]any{"group", groupName, "zone", zone, "outcome", "already_absent"}, requestContextLogArgs(requestContext)...)...)
			return nil
		}
		return err
	}

	if err := filesystem.RemoveUserGroup(group.Name, group.Zone); err != nil {
		normalizedErr := normalizeUserGroupError("delete group", groupName, zone, err)
		if options.Reconcile && errors.Is(normalizedErr, ErrNotFound) {
			slog.Info("usergroup DeleteUserGroup reconciled absent group", append([]any{"group", groupName, "zone", zone, "outcome", "already_absent"}, requestContextLogArgs(requestContext)...)...)
			return nil
		}
		logIRODSError("usergroup DeleteUserGroup failed", normalizedErr, append([]any{"group", groupName, "zone", zone, "reconcile", options.Reconcile}, requestContextLogArgs(requestContext)...)...)
		return normalizedErr
	}

	slog.Info("usergroup DeleteUserGroup completed", append([]any{"group", groupName, "zone", zone, "outcome", "deleted"}, requestContextLogArgs(requestContext)...)...)
	return nil
}

func (s *userGroupService) AddUserToGroup(_ context.Context, requestContext *RequestContext, groupName string, username string, zone string, options UserGroupMutationOptions) (domain.UserGroup, error) {
	groupName = strings.TrimSpace(groupName)
	username = strings.TrimSpace(username)
	zone = s.userZone(zone)
	if groupName == "" {
		return domain.UserGroup{}, fmt.Errorf("%w: group %q", ErrNotFound, groupName)
	}
	if username == "" {
		return domain.UserGroup{}, fmt.Errorf("%w: user %q", ErrNotFound, username)
	}

	slog.Debug("usergroup AddUserToGroup start", append([]any{"group", groupName, "user", username, "zone", zone, "reconcile", options.Reconcile}, requestContextLogArgs(requestContext)...)...)

	filesystem, err := s.filesystemForRequest(requestContext, "irods-go-rest-add-user-to-group")
	if err != nil {
		logIRODSError("usergroup AddUserToGroup filesystem setup failed", err, append([]any{"group", groupName, "user", username, "zone", zone, "reconcile", options.Reconcile}, requestContextLogArgs(requestContext)...)...)
		return domain.UserGroup{}, err
	}
	defer filesystem.Release()

	if err := s.requireManageUserGroupsPermission(filesystem, requestContext); err != nil {
		return domain.UserGroup{}, err
	}

	group, err := s.getGroup(filesystem, groupName, zone)
	if err != nil {
		return domain.UserGroup{}, err
	}

	user, err := filesystem.GetUser(username, zone, "")
	if err != nil {
		logIRODSError("usergroup AddUserToGroup get user failed", err, append([]any{"group", groupName, "user", username, "zone", zone, "reconcile", options.Reconcile}, requestContextLogArgs(requestContext)...)...)
		return domain.UserGroup{}, normalizeUserError("get user", username, zone, err)
	}
	if user == nil || strings.TrimSpace(string(user.Type)) == string(irodstypes.IRODSUserRodsGroup) {
		return domain.UserGroup{}, fmt.Errorf("%w: user %q", ErrNotFound, username)
	}

	if err := filesystem.AddGroupMember(group.Name, user.Name, zone); err != nil {
		normalizedErr := normalizeUserGroupError("add group member", groupName, zone, err)
		if options.Reconcile && errors.Is(normalizedErr, ErrConflict) {
			slog.Info("usergroup AddUserToGroup reconciled existing membership", append([]any{"group", groupName, "user", username, "zone", zone, "outcome", "already_member"}, requestContextLogArgs(requestContext)...)...)
			return s.groupWithMembers(filesystem, group)
		}
		logIRODSError("usergroup AddUserToGroup failed", normalizedErr, append([]any{"group", groupName, "user", username, "zone", zone, "reconcile", options.Reconcile}, requestContextLogArgs(requestContext)...)...)
		return domain.UserGroup{}, normalizedErr
	}

	group, err = s.groupWithMembers(filesystem, group)
	if err != nil {
		return domain.UserGroup{}, err
	}
	slog.Info("usergroup AddUserToGroup completed", append([]any{"group", groupName, "user", username, "zone", zone, "outcome", "added"}, requestContextLogArgs(requestContext)...)...)
	return group, nil
}

func (s *userGroupService) RemoveUserFromGroup(_ context.Context, requestContext *RequestContext, groupName string, username string, zone string, options UserGroupMutationOptions) (domain.UserGroup, error) {
	groupName = strings.TrimSpace(groupName)
	username = strings.TrimSpace(username)
	zone = s.userZone(zone)
	if groupName == "" {
		return domain.UserGroup{}, fmt.Errorf("%w: group %q", ErrNotFound, groupName)
	}
	if username == "" {
		return domain.UserGroup{}, fmt.Errorf("%w: user %q", ErrNotFound, username)
	}

	slog.Debug("usergroup RemoveUserFromGroup start", append([]any{"group", groupName, "user", username, "zone", zone, "reconcile", options.Reconcile}, requestContextLogArgs(requestContext)...)...)

	filesystem, err := s.filesystemForRequest(requestContext, "irods-go-rest-remove-user-from-group")
	if err != nil {
		logIRODSError("usergroup RemoveUserFromGroup filesystem setup failed", err, append([]any{"group", groupName, "user", username, "zone", zone, "reconcile", options.Reconcile}, requestContextLogArgs(requestContext)...)...)
		return domain.UserGroup{}, err
	}
	defer filesystem.Release()

	if err := s.requireManageUserGroupsPermission(filesystem, requestContext); err != nil {
		return domain.UserGroup{}, err
	}

	group, err := s.getGroup(filesystem, groupName, zone)
	if err != nil {
		return domain.UserGroup{}, err
	}

	if err := filesystem.RemoveGroupMember(group.Name, username, zone); err != nil {
		normalizedErr := normalizeUserGroupError("remove group member", groupName, zone, err)
		if options.Reconcile && errors.Is(normalizedErr, ErrNotFound) {
			slog.Info("usergroup RemoveUserFromGroup reconciled absent membership", append([]any{"group", groupName, "user", username, "zone", zone, "outcome", "already_absent"}, requestContextLogArgs(requestContext)...)...)
			return s.groupWithMembers(filesystem, group)
		}
		logIRODSError("usergroup RemoveUserFromGroup failed", normalizedErr, append([]any{"group", groupName, "user", username, "zone", zone, "reconcile", options.Reconcile}, requestContextLogArgs(requestContext)...)...)
		return domain.UserGroup{}, normalizedErr
	}

	group, err = s.groupWithMembers(filesystem, group)
	if err != nil {
		return domain.UserGroup{}, err
	}
	slog.Info("usergroup RemoveUserFromGroup completed", append([]any{"group", groupName, "user", username, "zone", zone, "outcome", "removed"}, requestContextLogArgs(requestContext)...)...)
	return group, nil
}

func (s *userGroupService) filesystemForRequest(requestContext *RequestContext, applicationName string) (CatalogFileSystem, error) {
	catalog := &catalogService{
		cfg:              s.cfg,
		createFileSystem: s.createFileSystem,
	}
	return catalog.filesystemForRequest(requestContext, applicationName)
}

func (s *userGroupService) requireManageUserGroupsPermission(filesystem CatalogFileSystem, requestContext *RequestContext) error {
	username := strings.TrimSpace(safeUsername(requestContext))
	if username == "" {
		return fmt.Errorf("%w: user group create/delete requires rodsadmin or groupadmin", ErrPermissionDenied)
	}

	user, err := filesystem.GetUser(username, s.userZone(""), "")
	if err != nil {
		return fmt.Errorf("%w: user group create/delete requires rodsadmin or groupadmin", ErrPermissionDenied)
	}
	if user == nil {
		return fmt.Errorf("%w: user group create/delete requires rodsadmin or groupadmin", ErrPermissionDenied)
	}
	if user.Type != irodstypes.IRODSUserRodsAdmin && user.Type != irodstypes.IRODSUserGroupAdmin {
		return fmt.Errorf("%w: user group create/delete requires rodsadmin or groupadmin", ErrPermissionDenied)
	}

	return nil
}

func (s *userGroupService) getGroup(filesystem CatalogFileSystem, groupName string, zone string) (domain.UserGroup, error) {
	group, err := filesystem.GetUser(groupName, zone, irodstypes.IRODSUserRodsGroup)
	if err != nil {
		return domain.UserGroup{}, normalizeUserGroupError("get group", groupName, zone, err)
	}

	mapped := mapUserGroup(group)
	if mapped.Name == "" || mapped.Type != string(irodstypes.IRODSUserRodsGroup) {
		return domain.UserGroup{}, fmt.Errorf("%w: group %q", ErrNotFound, groupName)
	}

	return mapped, nil
}

func (s *userGroupService) groupWithMembers(filesystem CatalogFileSystem, group domain.UserGroup) (domain.UserGroup, error) {
	users, err := filesystem.ListGroupMembers(group.Zone, group.Name)
	if err != nil {
		return domain.UserGroup{}, normalizeUserGroupError("list group members", group.Name, group.Zone, err)
	}

	group.Members = mapUserGroupMembers(users)
	return group, nil
}

func (s *userGroupService) userZone(zone string) string {
	zone = strings.TrimSpace(zone)
	if zone != "" {
		return zone
	}
	return strings.TrimSpace(s.cfg.IrodsZone)
}

func mapUserGroup(user *irodstypes.IRODSUser) domain.UserGroup {
	if user == nil {
		return domain.UserGroup{}
	}

	return domain.UserGroup{
		ID:   user.ID,
		Name: strings.TrimSpace(user.Name),
		Zone: strings.TrimSpace(user.Zone),
		Type: strings.TrimSpace(string(user.Type)),
	}
}

func mapUserGroupMembers(users []*irodstypes.IRODSUser) []domain.UserGroupMember {
	if len(users) == 0 {
		return []domain.UserGroupMember{}
	}

	members := make([]domain.UserGroupMember, 0, len(users))
	for _, user := range users {
		if user == nil {
			continue
		}

		mapped := mapUser(user)
		if mapped.Name == "" {
			continue
		}
		if mapped.Type == string(irodstypes.IRODSUserRodsGroup) {
			continue
		}

		members = append(members, domain.UserGroupMember{
			ID:   mapped.ID,
			Name: mapped.Name,
			Zone: mapped.Zone,
			Type: mapped.Type,
		})
	}

	sort.SliceStable(members, func(i, j int) bool {
		if members[i].Name != members[j].Name {
			return members[i].Name < members[j].Name
		}
		if members[i].Zone != members[j].Zone {
			return members[i].Zone < members[j].Zone
		}
		return members[i].Type < members[j].Type
	})

	return members
}

func sortUserGroups(groups []domain.UserGroup) {
	sort.SliceStable(groups, func(i, j int) bool {
		if groups[i].Name != groups[j].Name {
			return groups[i].Name < groups[j].Name
		}
		if groups[i].Zone != groups[j].Zone {
			return groups[i].Zone < groups[j].Zone
		}
		return groups[i].Type < groups[j].Type
	})
}

func normalizeUserGroupError(operation string, groupName string, zone string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrNotFound) || errors.Is(err, ErrPermissionDenied) || errors.Is(err, ErrConflict) {
		return err
	}

	switch irodstypes.GetIRODSErrorCode(err) {
	case irodscommon.CAT_NO_ACCESS_PERMISSION, irodscommon.SYS_NO_API_PRIV:
		return fmt.Errorf("%w: group %q", ErrPermissionDenied, groupName)
	case irodscommon.CAT_NO_ROWS_FOUND, irodsUserNotInGroupErrorCode:
		return fmt.Errorf("%w: group %q", ErrNotFound, groupName)
	case irodscommon.CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME:
		return fmt.Errorf("%w: group %q", ErrConflict, groupName)
	}

	if irodstypes.IsUserNotFoundError(err) {
		return fmt.Errorf("%w: group %q", ErrNotFound, groupName)
	}

	message := strings.ToLower(err.Error())
	if strings.Contains(message, "not found") ||
		strings.Contains(message, "no rows") ||
		strings.Contains(message, "not a member") ||
		strings.Contains(message, "not in group") ||
		strings.Contains(message, "unknown errorcode: -1830000") {
		return fmt.Errorf("%w: group %q", ErrNotFound, groupName)
	}
	if strings.Contains(message, "already exists") ||
		strings.Contains(message, "already has item by that name") ||
		strings.Contains(message, "catalog_already_has_item_by_that_name") ||
		strings.Contains(message, "exists as") {
		return fmt.Errorf("%w: group %q", ErrConflict, groupName)
	}
	if strings.Contains(message, "no access permission") || strings.Contains(message, "permission denied") || strings.Contains(message, "not authorized") {
		return fmt.Errorf("%w: group %q", ErrPermissionDenied, groupName)
	}

	if strings.TrimSpace(zone) != "" {
		return fmt.Errorf("%s group %q in zone %q: %w", operation, groupName, zone, err)
	}
	return fmt.Errorf("%s group %q: %w", operation, groupName, err)
}

func requestContextLogArgs(requestContext *RequestContext) []any {
	args := []any{
		"auth_scheme", safeAuthScheme(requestContext),
		"username", safeUsername(requestContext),
	}
	if requestContext == nil {
		return args
	}

	args = appendNonEmptyIRODSLogArg(args, "request_id", requestContext.RequestID)
	args = appendNonEmptyIRODSLogArg(args, "request_source", requestContext.RequestSource)
	args = appendNonEmptyIRODSLogArg(args, "request_actor", requestContext.RequestActor)
	args = appendNonEmptyIRODSLogArg(args, "idempotency_key", requestContext.IdempotencyKey)
	args = appendNonEmptyIRODSLogArg(args, "auth_subject", requestContext.Subject)
	args = appendNonEmptyIRODSLogArg(args, "auth_client_id", requestContext.ClientID)
	args = appendNonEmptyIRODSLogArg(args, "auth_scope", strings.Join(requestContext.Scopes, " "))
	args = appendNonEmptyIRODSLogArg(args, "auth_audience", strings.Join(requestContext.Audience, " "))
	return args
}

func appendNonEmptyIRODSLogArg(args []any, key string, value string) []any {
	value = strings.TrimSpace(value)
	if value == "" {
		return args
	}
	return append(args, key, value)
}
