package irods

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	irodstypes "github.com/cyverse/go-irodsclient/irods/types"
	usersandgroupsext "github.com/michael-conway/go-irodsclient-extensions/usersandgroups"
	usersyncext "github.com/michael-conway/go-irodsclient-extensions/usersync"
	"github.com/michael-conway/irods-go-rest/internal/config"
	"github.com/michael-conway/irods-go-rest/internal/domain"
)

type UserListOptions struct {
	Zone   string
	Type   string
	Prefix string
}

type UserUpdateOptions struct {
	Zone           string
	Type           string
	Password       string
	ChangeType     bool
	ChangePassword bool
}

type UserCreateOptions struct {
	Zone     string
	Type     string
	Password string
}

type UserMutationOptions struct {
	Reconcile bool
}

type UserService interface {
	ListUsers(ctx context.Context, requestContext *RequestContext, options UserListOptions) ([]domain.User, error)
	GetUser(ctx context.Context, requestContext *RequestContext, username string, zone string) (domain.User, error)
	GetUserMetadata(ctx context.Context, requestContext *RequestContext, username string, zone string) ([]domain.AVUMetadata, error)
	AddUserMetadata(ctx context.Context, requestContext *RequestContext, username string, zone string, attrib string, value string, unit string) (domain.AVUMetadata, error)
	UpdateUserMetadata(ctx context.Context, requestContext *RequestContext, username string, zone string, avuID string, attrib string, value string, unit string) (domain.AVUMetadata, error)
	DeleteUserMetadata(ctx context.Context, requestContext *RequestContext, username string, zone string, avuID string) error
	CreateUser(ctx context.Context, requestContext *RequestContext, username string, options UserCreateOptions, mutation UserMutationOptions) (domain.User, error)
	UpdateUser(ctx context.Context, requestContext *RequestContext, username string, options UserUpdateOptions, mutation UserMutationOptions) (domain.User, error)
	DeleteUser(ctx context.Context, requestContext *RequestContext, username string, zone string, mutation UserMutationOptions) error
}

type userService struct {
	cfg              config.RestConfig
	createFileSystem CatalogFileSystemFactory
}

func NewUserService(cfg config.RestConfig) UserService {
	return NewUserServiceWithFactory(cfg, defaultCatalogFileSystemFactory())
}

func NewUserServiceWithFactory(cfg config.RestConfig, factory CatalogFileSystemFactory) UserService {
	return &userService{
		cfg:              cfg,
		createFileSystem: factory,
	}
}

func (s *userService) ListUsers(_ context.Context, requestContext *RequestContext, options UserListOptions) ([]domain.User, error) {
	zone := s.userZone(options.Zone)
	prefix := strings.TrimSpace(options.Prefix)
	userTypes := userTypesForList(options.Type)

	slog.Debug("user ListUsers start", "zone", zone, "prefix", prefix, "type", options.Type, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))

	catalog := &catalogService{
		cfg:              s.cfg,
		createFileSystem: s.createFileSystem,
	}

	filesystem, err := catalog.filesystemForRequest(requestContext, "irods-go-rest-list-users")
	if err != nil {
		logIRODSError("user ListUsers filesystem setup failed", err, "zone", zone, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return nil, err
	}
	defer filesystem.Release()

	users := make([]domain.User, 0)
	for _, userType := range userTypes {
		typedUsers, err := filesystem.ListUsers(zone, userType)
		if err != nil {
			logIRODSError("user ListUsers failed", err, "zone", zone, "type", userType, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
			return nil, normalizeUserError("list users", "", zone, err)
		}

		for _, user := range typedUsers {
			mapped := mapUser(user)
			if mapped.Name == "" || !isUserType(mapped.Type) {
				continue
			}
			if prefix != "" && !strings.HasPrefix(mapped.Name, prefix) {
				continue
			}
			users = append(users, mapped)
		}
	}

	sortUsers(users)
	return users, nil
}

func (s *userService) GetUser(_ context.Context, requestContext *RequestContext, username string, zone string) (domain.User, error) {
	username = strings.TrimSpace(username)
	zone = s.userZone(zone)
	if username == "" {
		return domain.User{}, fmt.Errorf("%w: user %q", ErrNotFound, username)
	}

	slog.Debug("user GetUser start", "user", username, "zone", zone, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))

	filesystem, err := s.filesystemForRequest(requestContext, "irods-go-rest-get-user")
	if err != nil {
		logIRODSError("user GetUser filesystem setup failed", err, "user", username, "zone", zone, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.User{}, err
	}
	defer filesystem.Release()

	user, err := filesystem.GetUser(username, zone, "")
	if err != nil {
		logIRODSError("user GetUser failed", err, "user", username, "zone", zone, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.User{}, normalizeUserError("get user", username, zone, err)
	}

	mapped := mapUser(user)
	if !isUserType(mapped.Type) {
		return domain.User{}, fmt.Errorf("%w: user %q", ErrNotFound, username)
	}
	return mapped, nil
}

func (s *userService) GetUserMetadata(_ context.Context, requestContext *RequestContext, username string, zone string) ([]domain.AVUMetadata, error) {
	return listPrincipalMetadata(
		requestContext,
		username,
		zone,
		"irods-go-rest-get-user-metadata",
		s.userMetadataFilesystem,
		normalizeUserError,
	)
}

func (s *userService) AddUserMetadata(_ context.Context, requestContext *RequestContext, username string, zone string, attrib string, value string, unit string) (domain.AVUMetadata, error) {
	return addPrincipalMetadata(
		requestContext,
		username,
		zone,
		attrib,
		value,
		unit,
		"irods-go-rest-add-user-metadata",
		s.userMetadataFilesystem,
		normalizeUserError,
	)
}

func (s *userService) UpdateUserMetadata(_ context.Context, requestContext *RequestContext, username string, zone string, avuID string, attrib string, value string, unit string) (domain.AVUMetadata, error) {
	return updatePrincipalMetadata(
		requestContext,
		username,
		zone,
		avuID,
		attrib,
		value,
		unit,
		"irods-go-rest-update-user-metadata",
		s.userMetadataFilesystem,
		normalizeUserError,
		"user",
	)
}

func (s *userService) DeleteUserMetadata(_ context.Context, requestContext *RequestContext, username string, zone string, avuID string) error {
	return deletePrincipalMetadata(
		requestContext,
		username,
		zone,
		avuID,
		"irods-go-rest-delete-user-metadata",
		s.userMetadataFilesystem,
		normalizeUserError,
		"user",
	)
}

func (s *userService) CreateUser(ctx context.Context, requestContext *RequestContext, username string, options UserCreateOptions, mutation UserMutationOptions) (domain.User, error) {
	username = strings.TrimSpace(username)
	zone := s.userZone(options.Zone)
	userType := irodstypes.IRODSUserType(strings.TrimSpace(options.Type))
	if username == "" {
		return domain.User{}, fmt.Errorf("%w: user %q", ErrNotFound, username)
	}
	if !isUserType(string(userType)) {
		return domain.User{}, fmt.Errorf("invalid user type %q", options.Type)
	}

	slog.Debug("user CreateUser start", append([]any{"user", username, "zone", zone, "type", userType, "reconcile", mutation.Reconcile}, requestContextLogArgs(requestContext)...)...)

	filesystem, err := s.filesystemForRequest(requestContext, "irods-go-rest-create-user")
	if err != nil {
		logIRODSError("user CreateUser filesystem setup failed", err, append([]any{"user", username, "zone", zone, "type", userType, "reconcile", mutation.Reconcile}, requestContextLogArgs(requestContext)...)...)
		return domain.User{}, err
	}
	defer filesystem.Release()

	actorType, err := s.requireCreateUserPermission(filesystem, requestContext)
	if err != nil {
		return domain.User{}, err
	}
	if err := s.requireCreateUserTypePermission(actorType, userType, mutation); err != nil {
		return domain.User{}, err
	}

	if mutation.Reconcile {
		result, err := newUserSyncService(filesystem, zone, requestContext).EnsureUser(ctx, usersyncext.EnsureUserRequest{
			Name:     username,
			Zone:     zone,
			Type:     userType,
			Password: options.Password,
		})
		if err != nil {
			mappedErr := mapUserSyncError(err)
			logIRODSError("user CreateUser reconcile failed", mappedErr, append([]any{"user", username, "zone", zone, "type", userType, "reconcile", mutation.Reconcile}, requestContextLogArgs(requestContext)...)...)
			return domain.User{}, mappedErr
		}

		slog.Info("user CreateUser completed", append([]any{"user", username, "zone", zone, "type", userType, "outcome", string(result.Outcome)}, requestContextLogArgs(requestContext)...)...)
		return mapUserSyncUser(result.User), nil
	}

	if actorType == irodstypes.IRODSUserGroupAdmin {
		return s.createRodsUserAsGroupAdmin(ctx, filesystem, requestContext, username, zone, options.Password)
	}

	if _, err := filesystem.CreateUser(username, zone, userType); err != nil {
		normalizedErr := normalizeUserError("create user", username, zone, err)
		logIRODSError("user CreateUser create failed", normalizedErr, append([]any{"user", username, "zone", zone, "type", userType, "reconcile", mutation.Reconcile}, requestContextLogArgs(requestContext)...)...)
		return domain.User{}, normalizedErr
	}

	password := strings.TrimSpace(options.Password)
	if password != "" {
		if err := filesystem.ChangeUserPassword(username, zone, options.Password); err != nil {
			logIRODSError("user CreateUser set password failed", err, append([]any{"user", username, "zone", zone, "type", userType, "reconcile", mutation.Reconcile}, requestContextLogArgs(requestContext)...)...)
			return domain.User{}, normalizeUserError("create user password", username, zone, err)
		}
	}

	created, err := filesystem.GetUser(username, zone, "")
	if err != nil {
		logIRODSError("user CreateUser get created failed", err, append([]any{"user", username, "zone", zone, "type", userType, "reconcile", mutation.Reconcile}, requestContextLogArgs(requestContext)...)...)
		return domain.User{}, normalizeUserError("get user", username, zone, err)
	}

	slog.Info("user CreateUser completed", append([]any{"user", username, "zone", zone, "type", userType, "outcome", "created"}, requestContextLogArgs(requestContext)...)...)
	return mapUser(created), nil
}

func (s *userService) createRodsUserAsGroupAdmin(ctx context.Context, filesystem CatalogFileSystem, requestContext *RequestContext, username string, zone string, password string) (domain.User, error) {
	if strings.TrimSpace(password) == "" {
		return domain.User{}, newInvalidRequestError("groupadmin user create requires an initial password")
	}

	service := usersandgroupsext.NewService(filesystem.UsersAndGroupsCatalog(), zone)
	created, err := service.CreateRodsUserWithPassword(ctx, usersandgroupsext.CreateRodsUserWithPasswordRequest{
		Zone:     zone,
		Name:     username,
		Password: password,
	})
	if err != nil {
		if errors.Is(err, usersandgroupsext.ErrInvalidRequest) || errors.Is(err, usersandgroupsext.ErrMissingCatalog) {
			err = newInvalidRequestError(err.Error())
		} else {
			err = normalizeUserError("groupadmin create user", username, zone, err)
		}
		logIRODSError("user CreateUser groupadmin mkuser failed", err, append([]any{"user", username, "zone", zone, "type", irodstypes.IRODSUserRodsUser, "reconcile", false}, requestContextLogArgs(requestContext)...)...)
		return domain.User{}, err
	}

	slog.Info("user CreateUser completed", append([]any{"user", username, "zone", zone, "type", created.Type, "outcome", "created"}, requestContextLogArgs(requestContext)...)...)
	return domain.User{
		ID:   created.ID,
		Name: created.Name,
		Zone: created.Zone,
		Type: string(created.Type),
	}, nil
}

func (s *userService) UpdateUser(ctx context.Context, requestContext *RequestContext, username string, options UserUpdateOptions, mutation UserMutationOptions) (domain.User, error) {
	username = strings.TrimSpace(username)
	zone := s.userZone(options.Zone)
	if username == "" {
		return domain.User{}, fmt.Errorf("%w: user %q", ErrNotFound, username)
	}

	slog.Debug("user UpdateUser start", append([]any{"user", username, "zone", zone, "change_type", options.ChangeType, "change_password", options.ChangePassword, "reconcile", mutation.Reconcile}, requestContextLogArgs(requestContext)...)...)

	filesystem, err := s.filesystemForRequest(requestContext, "irods-go-rest-update-user")
	if err != nil {
		logIRODSError("user UpdateUser filesystem setup failed", err, append([]any{"user", username, "zone", zone, "change_type", options.ChangeType, "change_password", options.ChangePassword, "reconcile", mutation.Reconcile}, requestContextLogArgs(requestContext)...)...)
		return domain.User{}, err
	}
	defer filesystem.Release()

	actorType, err := s.authenticatedUserType(filesystem, requestContext, "user update")
	if err != nil {
		return domain.User{}, err
	}

	if mutation.Reconcile && actorType == irodstypes.IRODSUserRodsAdmin {
		result, err := newUserSyncService(filesystem, zone, requestContext).UpdateUser(ctx, usersyncext.UpdateUserRequest{
			Name:            username,
			Zone:            zone,
			Type:            irodstypes.IRODSUserType(strings.TrimSpace(options.Type)),
			Password:        options.Password,
			ChangeType:      options.ChangeType,
			ChangePassword:  options.ChangePassword,
			CreateIfMissing: true,
		})
		if err != nil {
			mappedErr := mapUserSyncError(err)
			logIRODSError("user UpdateUser reconcile failed", mappedErr, append([]any{"user", username, "zone", zone, "change_type", options.ChangeType, "change_password", options.ChangePassword, "reconcile", mutation.Reconcile}, requestContextLogArgs(requestContext)...)...)
			return domain.User{}, mappedErr
		}

		slog.Info("user UpdateUser completed", append([]any{"user", username, "zone", zone, "outcome", string(result.Outcome)}, requestContextLogArgs(requestContext)...)...)
		return mapUserSyncUser(result.User), nil
	}

	existing, err := filesystem.GetUser(username, zone, "")
	if err != nil {
		normalizedErr := normalizeUserError("get user", username, zone, err)
		logIRODSError("user UpdateUser get target failed", normalizedErr, append([]any{"user", username, "zone", zone, "change_type", options.ChangeType, "change_password", options.ChangePassword, "reconcile", mutation.Reconcile}, requestContextLogArgs(requestContext)...)...)
		return domain.User{}, normalizedErr
	}
	if !isUserType(string(existing.Type)) {
		return domain.User{}, fmt.Errorf("%w: user %q", ErrNotFound, username)
	}

	if options.ChangeType {
		newType := irodstypes.IRODSUserType(strings.TrimSpace(options.Type))
		if existing.Type != newType {
			if err := filesystem.ChangeUserType(username, zone, newType); err != nil {
				logIRODSError("user UpdateUser change type failed", err, append([]any{"user", username, "zone", zone, "type", newType, "reconcile", mutation.Reconcile}, requestContextLogArgs(requestContext)...)...)
				return domain.User{}, normalizeUserError("update user type", username, zone, err)
			}
		}
		existing.Type = newType
	}

	if options.ChangePassword {
		if err := filesystem.ChangeUserPassword(username, zone, options.Password); err != nil {
			logIRODSError("user UpdateUser change password failed", err, append([]any{"user", username, "zone", zone, "reconcile", mutation.Reconcile}, requestContextLogArgs(requestContext)...)...)
			return domain.User{}, normalizeUserError("update user password", username, zone, err)
		}
	}

	slog.Info("user UpdateUser completed", append([]any{"user", username, "zone", zone, "outcome", "updated"}, requestContextLogArgs(requestContext)...)...)
	return mapUser(existing), nil
}

func (s *userService) DeleteUser(ctx context.Context, requestContext *RequestContext, username string, zone string, mutation UserMutationOptions) error {
	username = strings.TrimSpace(username)
	zone = s.userZone(zone)
	if username == "" {
		return fmt.Errorf("%w: user %q", ErrNotFound, username)
	}

	slog.Debug("user DeleteUser start", append([]any{"user", username, "zone", zone, "reconcile", mutation.Reconcile}, requestContextLogArgs(requestContext)...)...)

	filesystem, err := s.filesystemForRequest(requestContext, "irods-go-rest-delete-user")
	if err != nil {
		logIRODSError("user DeleteUser filesystem setup failed", err, append([]any{"user", username, "zone", zone, "reconcile", mutation.Reconcile}, requestContextLogArgs(requestContext)...)...)
		return err
	}
	defer filesystem.Release()

	if err := s.requireCreateDeleteUserPermission(filesystem, requestContext); err != nil {
		return err
	}

	if mutation.Reconcile {
		result, err := newUserSyncService(filesystem, zone, requestContext).EnsureUserAbsent(ctx, usersyncext.UserRef{
			Name: username,
			Zone: zone,
		})
		if err != nil {
			mappedErr := mapUserSyncError(err)
			logIRODSError("user DeleteUser reconcile failed", mappedErr, append([]any{"user", username, "zone", zone, "reconcile", mutation.Reconcile}, requestContextLogArgs(requestContext)...)...)
			return mappedErr
		}

		slog.Info("user DeleteUser completed", append([]any{"user", username, "zone", zone, "type", result.User.Type, "outcome", string(result.Outcome)}, requestContextLogArgs(requestContext)...)...)
		return nil
	}

	user, err := filesystem.GetUser(username, zone, "")
	if err != nil {
		normalizedErr := normalizeUserError("get user", username, zone, err)
		logIRODSError("user DeleteUser get target failed", normalizedErr, append([]any{"user", username, "zone", zone, "reconcile", mutation.Reconcile}, requestContextLogArgs(requestContext)...)...)
		return normalizedErr
	}
	if !isUserType(string(user.Type)) {
		return fmt.Errorf("%w: user %q", ErrNotFound, username)
	}

	if err := filesystem.RemoveUser(username, zone, user.Type); err != nil {
		normalizedErr := normalizeUserError("delete user", username, zone, err)
		logIRODSError("user DeleteUser remove failed", normalizedErr, append([]any{"user", username, "zone", zone, "type", user.Type, "reconcile", mutation.Reconcile}, requestContextLogArgs(requestContext)...)...)
		return normalizedErr
	}

	slog.Info("user DeleteUser completed", append([]any{"user", username, "zone", zone, "type", user.Type, "outcome", "deleted"}, requestContextLogArgs(requestContext)...)...)
	return nil
}

func (s *userService) filesystemForRequest(requestContext *RequestContext, applicationName string) (CatalogFileSystem, error) {
	catalog := &catalogService{
		cfg:              s.cfg,
		createFileSystem: s.createFileSystem,
	}
	return catalog.filesystemForRequest(requestContext, applicationName)
}

func (s *userService) userMetadataFilesystem(requestContext *RequestContext, username string, zone string, applicationName string) (CatalogFileSystem, string, string, error) {
	username = strings.TrimSpace(username)
	zone = s.userZone(zone)
	if username == "" {
		return nil, "", "", fmt.Errorf("%w: user %q", ErrNotFound, username)
	}

	filesystem, err := s.filesystemForRequest(requestContext, applicationName)
	if err != nil {
		return nil, "", "", err
	}

	user, err := filesystem.GetUser(username, zone, "")
	if err != nil {
		filesystem.Release()
		return nil, "", "", normalizeUserError("get user", username, zone, err)
	}
	if user == nil || user.Type == irodstypes.IRODSUserRodsGroup {
		filesystem.Release()
		return nil, "", "", fmt.Errorf("%w: user %q", ErrNotFound, username)
	}

	return filesystem, username, zone, nil
}

func (s *userService) authenticatedUserType(filesystem CatalogFileSystem, requestContext *RequestContext, operation string) (irodstypes.IRODSUserType, error) {
	return authenticatedPrincipalType(
		filesystem,
		requestContext,
		s.userZone(""),
		operation,
		irodstypes.IRODSUserRodsAdmin,
	)
}

func (s *userService) requireCreateDeleteUserPermission(filesystem CatalogFileSystem, requestContext *RequestContext) error {
	_, err := authenticatedPrincipalType(
		filesystem,
		requestContext,
		s.userZone(""),
		"user create/delete",
		irodstypes.IRODSUserRodsAdmin,
	)
	return err
}

func (s *userService) requireCreateUserPermission(filesystem CatalogFileSystem, requestContext *RequestContext) (irodstypes.IRODSUserType, error) {
	return authenticatedPrincipalType(
		filesystem,
		requestContext,
		s.userZone(""),
		"user create",
		irodstypes.IRODSUserRodsAdmin,
		irodstypes.IRODSUserGroupAdmin,
	)
}

func (s *userService) requireCreateUserTypePermission(actorType irodstypes.IRODSUserType, userType irodstypes.IRODSUserType, mutation UserMutationOptions) error {
	if actorType == irodstypes.IRODSUserRodsAdmin {
		return nil
	}
	if actorType == irodstypes.IRODSUserGroupAdmin && !mutation.Reconcile && userType == irodstypes.IRODSUserRodsUser {
		return nil
	}
	return fmt.Errorf("%w: user create requires rodsadmin; groupadmin may create rodsuser only", ErrPermissionDenied)
}

func (s *userService) userZone(zone string) string {
	zone = strings.TrimSpace(zone)
	if zone != "" {
		return zone
	}
	return strings.TrimSpace(s.cfg.IrodsZone)
}

func mapUser(user *irodstypes.IRODSUser) domain.User {
	if user == nil {
		return domain.User{}
	}

	return domain.User{
		ID:   user.ID,
		Name: strings.TrimSpace(user.Name),
		Zone: strings.TrimSpace(user.Zone),
		Type: strings.TrimSpace(string(user.Type)),
	}
}

func userTypesForList(rawType string) []irodstypes.IRODSUserType {
	switch strings.TrimSpace(rawType) {
	case string(irodstypes.IRODSUserRodsUser):
		return []irodstypes.IRODSUserType{irodstypes.IRODSUserRodsUser}
	case string(irodstypes.IRODSUserGroupAdmin):
		return []irodstypes.IRODSUserType{irodstypes.IRODSUserGroupAdmin}
	case string(irodstypes.IRODSUserRodsAdmin):
		return []irodstypes.IRODSUserType{irodstypes.IRODSUserRodsAdmin}
	default:
		return []irodstypes.IRODSUserType{
			irodstypes.IRODSUserRodsUser,
			irodstypes.IRODSUserGroupAdmin,
			irodstypes.IRODSUserRodsAdmin,
		}
	}
}

func isUserType(userType string) bool {
	switch strings.TrimSpace(userType) {
	case string(irodstypes.IRODSUserRodsUser), string(irodstypes.IRODSUserGroupAdmin), string(irodstypes.IRODSUserRodsAdmin):
		return true
	default:
		return false
	}
}

func sortUsers(users []domain.User) {
	sort.SliceStable(users, func(i, j int) bool {
		if users[i].Name != users[j].Name {
			return users[i].Name < users[j].Name
		}
		if users[i].Zone != users[j].Zone {
			return users[i].Zone < users[j].Zone
		}
		return users[i].Type < users[j].Type
	})
}

func normalizeUserError(operation string, username string, zone string, err error) error {
	return mapUserSyncError(usersyncext.NormalizeUserError(operation, username, zone, err))
}
