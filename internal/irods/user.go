package irods

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	irodsfs "github.com/cyverse/go-irodsclient/fs"
	irodstypes "github.com/cyverse/go-irodsclient/irods/types"
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
	CreateUser(ctx context.Context, requestContext *RequestContext, username string, options UserCreateOptions, mutation UserMutationOptions) (domain.User, error)
	UpdateUser(ctx context.Context, requestContext *RequestContext, username string, options UserUpdateOptions, mutation UserMutationOptions) (domain.User, error)
	DeleteUser(ctx context.Context, requestContext *RequestContext, username string, zone string, mutation UserMutationOptions) error
}

type userService struct {
	cfg              config.RestConfig
	createFileSystem CatalogFileSystemFactory
}

func NewUserService(cfg config.RestConfig) UserService {
	return NewUserServiceWithFactory(cfg, func(account *irodstypes.IRODSAccount, applicationName string) (CatalogFileSystem, error) {
		filesystem, err := irodsfs.NewFileSystemWithDefault(account, applicationName)
		if err != nil {
			return nil, err
		}
		return &catalogFileSystemAdapter{filesystem: filesystem}, nil
	})
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

	if err := s.requireCreateDeleteUserPermission(filesystem, requestContext); err != nil {
		return domain.User{}, err
	}

	if mutation.Reconcile {
		result, err := usersyncext.NewService(filesystem, zone).EnsureUser(ctx, usersyncext.EnsureUserRequest{
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

	if err := s.requireRodsAdmin(filesystem, requestContext); err != nil {
		return domain.User{}, err
	}

	result, err := usersyncext.NewService(filesystem, zone).UpdateUser(ctx, usersyncext.UpdateUserRequest{
		Name:            username,
		Zone:            zone,
		Type:            irodstypes.IRODSUserType(strings.TrimSpace(options.Type)),
		Password:        options.Password,
		ChangeType:      options.ChangeType,
		ChangePassword:  options.ChangePassword,
		CreateIfMissing: mutation.Reconcile,
	})
	if err != nil {
		mappedErr := mapUserSyncError(err)
		logIRODSError("user UpdateUser failed", mappedErr, append([]any{"user", username, "zone", zone, "change_type", options.ChangeType, "change_password", options.ChangePassword, "reconcile", mutation.Reconcile}, requestContextLogArgs(requestContext)...)...)
		return domain.User{}, mappedErr
	}

	slog.Info("user UpdateUser completed", append([]any{"user", username, "zone", zone, "outcome", string(result.Outcome)}, requestContextLogArgs(requestContext)...)...)
	return mapUserSyncUser(result.User), nil
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
		result, err := usersyncext.NewService(filesystem, zone).EnsureUserAbsent(ctx, usersyncext.UserRef{
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

func (s *userService) requireRodsAdmin(filesystem CatalogFileSystem, requestContext *RequestContext) error {
	username := strings.TrimSpace(safeUsername(requestContext))
	if username == "" {
		return fmt.Errorf("%w: user update/delete requires rodsadmin", ErrPermissionDenied)
	}

	user, err := filesystem.GetUser(username, s.userZone(""), "")
	if err != nil {
		return fmt.Errorf("%w: user update/delete requires rodsadmin", ErrPermissionDenied)
	}
	if user == nil || user.Type != irodstypes.IRODSUserRodsAdmin {
		return fmt.Errorf("%w: user update/delete requires rodsadmin", ErrPermissionDenied)
	}

	return nil
}

func (s *userService) requireCreateDeleteUserPermission(filesystem CatalogFileSystem, requestContext *RequestContext) error {
	username := strings.TrimSpace(safeUsername(requestContext))
	if username == "" {
		return fmt.Errorf("%w: user create/delete requires rodsadmin or groupadmin", ErrPermissionDenied)
	}

	user, err := filesystem.GetUser(username, s.userZone(""), "")
	if err != nil {
		return fmt.Errorf("%w: user create/delete requires rodsadmin or groupadmin", ErrPermissionDenied)
	}
	if user == nil {
		return fmt.Errorf("%w: user create/delete requires rodsadmin or groupadmin", ErrPermissionDenied)
	}
	if user.Type != irodstypes.IRODSUserRodsAdmin && user.Type != irodstypes.IRODSUserGroupAdmin {
		return fmt.Errorf("%w: user create/delete requires rodsadmin or groupadmin", ErrPermissionDenied)
	}

	return nil
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
	case string(irodstypes.IRODSUserRodsAdmin):
		return []irodstypes.IRODSUserType{irodstypes.IRODSUserRodsAdmin}
	default:
		return []irodstypes.IRODSUserType{irodstypes.IRODSUserRodsUser, irodstypes.IRODSUserRodsAdmin}
	}
}

func isUserType(userType string) bool {
	switch strings.TrimSpace(userType) {
	case string(irodstypes.IRODSUserRodsUser), string(irodstypes.IRODSUserRodsAdmin):
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
