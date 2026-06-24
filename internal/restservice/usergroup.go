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

type UserGroupMutationOptions struct {
	Reconcile bool
}

type UserGroupService interface {
	ListUserGroups(ctx context.Context, options UserGroupListOptions) ([]domain.UserGroup, error)
	GetUserGroup(ctx context.Context, groupName string, zone string) (domain.UserGroup, error)
	GetUserGroupMetadata(ctx context.Context, groupName string, zone string) ([]domain.AVUMetadata, error)
	AddUserGroupMetadata(ctx context.Context, groupName string, zone string, attrib string, value string, unit string) (domain.AVUMetadata, error)
	UpdateUserGroupMetadata(ctx context.Context, groupName string, zone string, avuID string, attrib string, value string, unit string) (domain.AVUMetadata, error)
	DeleteUserGroupMetadata(ctx context.Context, groupName string, zone string, avuID string) error
	CreateUserGroup(ctx context.Context, groupName string, zone string, options UserGroupMutationOptions) (domain.UserGroup, error)
	DeleteUserGroup(ctx context.Context, groupName string, zone string, options UserGroupMutationOptions) error
	AddUserToGroup(ctx context.Context, groupName string, username string, zone string, options UserGroupMutationOptions) (domain.UserGroup, error)
	RemoveUserFromGroup(ctx context.Context, groupName string, username string, zone string, options UserGroupMutationOptions) (domain.UserGroup, error)
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

func (s *userGroupService) GetUserGroupMetadata(ctx context.Context, groupName string, zone string) ([]domain.AVUMetadata, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return nil, err
	}

	return s.userGroups.GetUserGroupMetadata(ctx, irodsRequestContext(requestContext), groupName, zone)
}

func (s *userGroupService) AddUserGroupMetadata(ctx context.Context, groupName string, zone string, attrib string, value string, unit string) (domain.AVUMetadata, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return domain.AVUMetadata{}, err
	}

	return s.userGroups.AddUserGroupMetadata(ctx, irodsRequestContext(requestContext), groupName, zone, attrib, value, unit)
}

func (s *userGroupService) UpdateUserGroupMetadata(ctx context.Context, groupName string, zone string, avuID string, attrib string, value string, unit string) (domain.AVUMetadata, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return domain.AVUMetadata{}, err
	}

	return s.userGroups.UpdateUserGroupMetadata(ctx, irodsRequestContext(requestContext), groupName, zone, avuID, attrib, value, unit)
}

func (s *userGroupService) DeleteUserGroupMetadata(ctx context.Context, groupName string, zone string, avuID string) error {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return err
	}

	return s.userGroups.DeleteUserGroupMetadata(ctx, irodsRequestContext(requestContext), groupName, zone, avuID)
}

func (s *userGroupService) CreateUserGroup(ctx context.Context, groupName string, zone string, options UserGroupMutationOptions) (domain.UserGroup, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return domain.UserGroup{}, err
	}

	return s.userGroups.CreateUserGroup(ctx, irodsRequestContext(requestContext), groupName, zone, irods.UserGroupMutationOptions{
		Reconcile: options.Reconcile,
	})
}

func (s *userGroupService) DeleteUserGroup(ctx context.Context, groupName string, zone string, options UserGroupMutationOptions) error {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return err
	}

	return s.userGroups.DeleteUserGroup(ctx, irodsRequestContext(requestContext), groupName, zone, irods.UserGroupMutationOptions{
		Reconcile: options.Reconcile,
	})
}

func (s *userGroupService) AddUserToGroup(ctx context.Context, groupName string, username string, zone string, options UserGroupMutationOptions) (domain.UserGroup, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return domain.UserGroup{}, err
	}

	return s.userGroups.AddUserToGroup(ctx, irodsRequestContext(requestContext), groupName, username, zone, irods.UserGroupMutationOptions{
		Reconcile: options.Reconcile,
	})
}

func (s *userGroupService) RemoveUserFromGroup(ctx context.Context, groupName string, username string, zone string, options UserGroupMutationOptions) (domain.UserGroup, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return domain.UserGroup{}, err
	}

	return s.userGroups.RemoveUserFromGroup(ctx, irodsRequestContext(requestContext), groupName, username, zone, irods.UserGroupMutationOptions{
		Reconcile: options.Reconcile,
	})
}
