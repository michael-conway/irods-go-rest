package restservice

import (
	"context"

	"github.com/michael-conway/irods-go-rest/internal/domain"
	"github.com/michael-conway/irods-go-rest/internal/irods"
)

type UserGroupListOptions struct {
	Zone   string
	Prefix string
}

type UserGroupService interface {
	ListUserGroups(ctx context.Context, options UserGroupListOptions) ([]domain.UserGroup, error)
	GetUserGroup(ctx context.Context, groupName string, zone string) (domain.UserGroup, error)
	CreateUserGroup(ctx context.Context, groupName string, zone string) (domain.UserGroup, error)
	DeleteUserGroup(ctx context.Context, groupName string, zone string) error
	AddUserToGroup(ctx context.Context, groupName string, username string, zone string) (domain.UserGroup, error)
	RemoveUserFromGroup(ctx context.Context, groupName string, username string, zone string) (domain.UserGroup, error)
}

type userGroupService struct {
	userGroups irods.UserGroupService
}

func NewUserGroupService(userGroups irods.UserGroupService) UserGroupService {
	return &userGroupService{userGroups: userGroups}
}

func (s *userGroupService) ListUserGroups(ctx context.Context, options UserGroupListOptions) ([]domain.UserGroup, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return nil, err
	}

	return s.userGroups.ListUserGroups(ctx, irodsRequestContext(requestContext), irods.UserGroupListOptions{
		Zone:   options.Zone,
		Prefix: options.Prefix,
	})
}

func (s *userGroupService) GetUserGroup(ctx context.Context, groupName string, zone string) (domain.UserGroup, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return domain.UserGroup{}, err
	}

	return s.userGroups.GetUserGroup(ctx, irodsRequestContext(requestContext), groupName, zone)
}

func (s *userGroupService) CreateUserGroup(ctx context.Context, groupName string, zone string) (domain.UserGroup, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return domain.UserGroup{}, err
	}

	return s.userGroups.CreateUserGroup(ctx, irodsRequestContext(requestContext), groupName, zone)
}

func (s *userGroupService) DeleteUserGroup(ctx context.Context, groupName string, zone string) error {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return err
	}

	return s.userGroups.DeleteUserGroup(ctx, irodsRequestContext(requestContext), groupName, zone)
}

func (s *userGroupService) AddUserToGroup(ctx context.Context, groupName string, username string, zone string) (domain.UserGroup, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return domain.UserGroup{}, err
	}

	return s.userGroups.AddUserToGroup(ctx, irodsRequestContext(requestContext), groupName, username, zone)
}

func (s *userGroupService) RemoveUserFromGroup(ctx context.Context, groupName string, username string, zone string) (domain.UserGroup, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return domain.UserGroup{}, err
	}

	return s.userGroups.RemoveUserFromGroup(ctx, irodsRequestContext(requestContext), groupName, username, zone)
}
