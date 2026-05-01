package restservice

import (
	"context"

	"github.com/michael-conway/irods-go-rest/internal/domain"
	"github.com/michael-conway/irods-go-rest/internal/irods"
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

type UserService interface {
	ListUsers(ctx context.Context, options UserListOptions) ([]domain.User, error)
	GetUser(ctx context.Context, username string, zone string) (domain.User, error)
	CreateUser(ctx context.Context, username string, options UserCreateOptions) (domain.User, error)
	UpdateUser(ctx context.Context, username string, options UserUpdateOptions) (domain.User, error)
	DeleteUser(ctx context.Context, username string, zone string) error
}

type userService struct {
	users irods.UserService
}

func NewUserService(users irods.UserService) UserService {
	return &userService{users: users}
}

func (s *userService) ListUsers(ctx context.Context, options UserListOptions) ([]domain.User, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return nil, err
	}

	return s.users.ListUsers(ctx, irodsRequestContext(requestContext), irods.UserListOptions{
		Zone:   options.Zone,
		Type:   options.Type,
		Prefix: options.Prefix,
	})
}

func (s *userService) GetUser(ctx context.Context, username string, zone string) (domain.User, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return domain.User{}, err
	}

	return s.users.GetUser(ctx, irodsRequestContext(requestContext), username, zone)
}

func (s *userService) CreateUser(ctx context.Context, username string, options UserCreateOptions) (domain.User, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return domain.User{}, err
	}

	return s.users.CreateUser(ctx, irodsRequestContext(requestContext), username, irods.UserCreateOptions{
		Zone:     options.Zone,
		Type:     options.Type,
		Password: options.Password,
	})
}

func (s *userService) UpdateUser(ctx context.Context, username string, options UserUpdateOptions) (domain.User, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return domain.User{}, err
	}

	return s.users.UpdateUser(ctx, irodsRequestContext(requestContext), username, irods.UserUpdateOptions{
		Zone:           options.Zone,
		Type:           options.Type,
		Password:       options.Password,
		ChangeType:     options.ChangeType,
		ChangePassword: options.ChangePassword,
	})
}

func (s *userService) DeleteUser(ctx context.Context, username string, zone string) error {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return err
	}

	return s.users.DeleteUser(ctx, irodsRequestContext(requestContext), username, zone)
}
