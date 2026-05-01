package irods

import (
	"context"
	"errors"
	"testing"

	irodstypes "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/michael-conway/irods-go-rest/internal/config"
)

func TestUserGroupListFiltersByPrefix(t *testing.T) {
	service := newTestUserGroupService(t, newCatalogTestFileSystem())

	groups, err := service.ListUserGroups(context.Background(), bearerRequestContext(), UserGroupListOptions{
		Prefix: "res",
	})
	if err != nil {
		t.Fatalf("ListUserGroups returned error: %v", err)
	}

	if len(groups) != 1 || groups[0].Name != "research-team" {
		t.Fatalf("unexpected groups: %+v", groups)
	}
}

func TestUserGroupCreateRequiresAdminOrGroupAdmin(t *testing.T) {
	service := newTestUserGroupService(t, newCatalogTestFileSystem())

	_, err := service.CreateUserGroup(context.Background(), bearerRequestContext(), "science-team", "tempZone")
	if !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("expected permission denied, got %v", err)
	}
}

func TestUserGroupCreateSucceedsForGroupAdmin(t *testing.T) {
	service := newTestUserGroupService(t, newCatalogTestFileSystem())

	group, err := service.CreateUserGroup(context.Background(), groupAdminRequestContext(), "science-team", "tempZone")
	if err != nil {
		t.Fatalf("CreateUserGroup returned error: %v", err)
	}
	if group.Name != "science-team" || group.Type != string(irodstypes.IRODSUserRodsGroup) {
		t.Fatalf("unexpected group: %+v", group)
	}
}

func TestUserGroupGetIncludesMembers(t *testing.T) {
	service := newTestUserGroupService(t, newCatalogTestFileSystem())

	group, err := service.GetUserGroup(context.Background(), bearerRequestContext(), "research-team", "tempZone")
	if err != nil {
		t.Fatalf("GetUserGroup returned error: %v", err)
	}
	if group.Name != "research-team" {
		t.Fatalf("unexpected group: %+v", group)
	}
	if len(group.Members) != 1 || group.Members[0].Name != "alice" {
		t.Fatalf("unexpected members: %+v", group.Members)
	}
}

func TestUserGroupAddAndRemoveMember(t *testing.T) {
	service := newTestUserGroupService(t, newCatalogTestFileSystem())

	added, err := service.AddUserToGroup(context.Background(), groupAdminRequestContext(), "research-team", "bob", "tempZone")
	if err != nil {
		t.Fatalf("AddUserToGroup returned error: %v", err)
	}
	if len(added.Members) < 2 {
		t.Fatalf("expected bob added, got %+v", added.Members)
	}

	removed, err := service.RemoveUserFromGroup(context.Background(), groupAdminRequestContext(), "research-team", "bob", "tempZone")
	if err != nil {
		t.Fatalf("RemoveUserFromGroup returned error: %v", err)
	}
	for _, member := range removed.Members {
		if member.Name == "bob" {
			t.Fatalf("expected bob removed, got %+v", removed.Members)
		}
	}
}

func newTestUserGroupService(t *testing.T, filesystem *catalogTestFileSystem) UserGroupService {
	t.Helper()

	cfg := config.RestConfig{
		IrodsZone:            "tempZone",
		IrodsHost:            "irods.local",
		IrodsPort:            1247,
		IrodsAuthScheme:      "native",
		IrodsAdminUser:       "rods",
		IrodsAdminPassword:   "rods",
		IrodsDefaultResource: "demoResc",
	}

	return NewUserGroupServiceWithFactory(cfg, func(_ *irodstypes.IRODSAccount, _ string) (CatalogFileSystem, error) {
		return filesystem, nil
	})
}
