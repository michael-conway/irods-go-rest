package irods

import (
	"context"
	"errors"
	"testing"

	irodscommon "github.com/cyverse/go-irodsclient/irods/common"
	irodstypes "github.com/cyverse/go-irodsclient/irods/types"
	usersyncext "github.com/michael-conway/go-irodsclient-extensions/usersync"
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

	_, err := service.CreateUserGroup(context.Background(), bearerRequestContext(), "science-team", "tempZone", UserGroupMutationOptions{})
	if !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("expected permission denied, got %v", err)
	}
}

func TestUserGroupCreateSucceedsForGroupAdmin(t *testing.T) {
	service := newTestUserGroupService(t, newCatalogTestFileSystem())

	group, err := service.CreateUserGroup(context.Background(), groupAdminRequestContext(), "science-team", "tempZone", UserGroupMutationOptions{})
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

	added, err := service.AddUserToGroup(context.Background(), groupAdminRequestContext(), "research-team", "bob", "tempZone", UserGroupMutationOptions{})
	if err != nil {
		t.Fatalf("AddUserToGroup returned error: %v", err)
	}
	if len(added.Members) < 2 {
		t.Fatalf("expected bob added, got %+v", added.Members)
	}

	removed, err := service.RemoveUserFromGroup(context.Background(), groupAdminRequestContext(), "research-team", "bob", "tempZone", UserGroupMutationOptions{})
	if err != nil {
		t.Fatalf("RemoveUserFromGroup returned error: %v", err)
	}
	for _, member := range removed.Members {
		if member.Name == "bob" {
			t.Fatalf("expected bob removed, got %+v", removed.Members)
		}
	}
}

func TestUserGroupCreateReconcileExistingGroup(t *testing.T) {
	service := newTestUserGroupService(t, newCatalogTestFileSystem())

	_, strictErr := service.CreateUserGroup(context.Background(), groupAdminRequestContext(), "research-team", "tempZone", UserGroupMutationOptions{})
	if !errors.Is(strictErr, ErrConflict) {
		t.Fatalf("expected strict duplicate create conflict, got %v", strictErr)
	}

	group, err := service.CreateUserGroup(context.Background(), groupAdminRequestContext(), "research-team", "tempZone", UserGroupMutationOptions{Reconcile: true})
	if err != nil {
		t.Fatalf("CreateUserGroup reconcile returned error: %v", err)
	}
	if group.Name != "research-team" {
		t.Fatalf("unexpected group: %+v", group)
	}
}

func TestUserGroupDeleteReconcileMissingGroup(t *testing.T) {
	service := newTestUserGroupService(t, newCatalogTestFileSystem())

	strictErr := service.DeleteUserGroup(context.Background(), groupAdminRequestContext(), "missing-team", "tempZone", UserGroupMutationOptions{})
	if !errors.Is(strictErr, ErrNotFound) {
		t.Fatalf("expected strict delete not found, got %v", strictErr)
	}

	err := service.DeleteUserGroup(context.Background(), groupAdminRequestContext(), "missing-team", "tempZone", UserGroupMutationOptions{Reconcile: true})
	if err != nil {
		t.Fatalf("DeleteUserGroup reconcile returned error: %v", err)
	}
}

func TestUserGroupAddReconcileExistingMember(t *testing.T) {
	service := newTestUserGroupService(t, newCatalogTestFileSystem())

	_, strictErr := service.AddUserToGroup(context.Background(), groupAdminRequestContext(), "research-team", "alice", "tempZone", UserGroupMutationOptions{})
	if !errors.Is(strictErr, ErrConflict) {
		t.Fatalf("expected strict duplicate member conflict, got %v", strictErr)
	}

	group, err := service.AddUserToGroup(context.Background(), groupAdminRequestContext(), "research-team", "alice", "tempZone", UserGroupMutationOptions{Reconcile: true})
	if err != nil {
		t.Fatalf("AddUserToGroup reconcile returned error: %v", err)
	}
	if len(group.Members) != 1 || group.Members[0].Name != "alice" {
		t.Fatalf("unexpected members: %+v", group.Members)
	}
}

func TestUserGroupRemoveReconcileMissingMember(t *testing.T) {
	filesystem := newCatalogTestFileSystem()
	filesystem.addUserMetadata("research-team", "tempZone", usersyncext.AVUAttributeManaged, usersyncext.AVUValueTrue, "")
	service := newTestUserGroupService(t, filesystem)

	_, strictErr := service.RemoveUserFromGroup(context.Background(), groupAdminRequestContext(), "research-team", "bob", "tempZone", UserGroupMutationOptions{})
	if !errors.Is(strictErr, ErrNotFound) {
		t.Fatalf("expected strict remove not found, got %v", strictErr)
	}

	group, err := service.RemoveUserFromGroup(context.Background(), groupAdminRequestContext(), "research-team", "bob", "tempZone", UserGroupMutationOptions{Reconcile: true})
	if err != nil {
		t.Fatalf("RemoveUserFromGroup reconcile returned error: %v", err)
	}
	if len(group.Members) != 1 || group.Members[0].Name != "alice" {
		t.Fatalf("unexpected members: %+v", group.Members)
	}
}

func TestUserGroupRemoveReconcileRejectsUnmanagedGroup(t *testing.T) {
	service := newTestUserGroupService(t, newCatalogTestFileSystem())

	_, err := service.RemoveUserFromGroup(context.Background(), groupAdminRequestContext(), "research-team", "bob", "tempZone", UserGroupMutationOptions{Reconcile: true})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("expected unmanaged group removal conflict, got %v", err)
	}
}

func TestNormalizeUserGroupErrorMapsCatalogAlreadyHasItemToConflict(t *testing.T) {
	err := normalizeUserGroupError("create group", "research-team", "tempZone", irodstypes.NewIRODSError(irodscommon.CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME))
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("expected catalog already-has-item code to map to conflict, got %v", err)
	}

	err = normalizeUserGroupError("create group", "research-team", "tempZone", errors.New("received create group error: CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME"))
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("expected catalog already-has-item message to map to conflict, got %v", err)
	}
}

func TestNormalizeUserGroupErrorMapsUserNotInGroupToNotFound(t *testing.T) {
	err := normalizeUserGroupError("remove group member", "research-team", "tempZone", irodstypes.NewIRODSError(irodscommon.ErrorCode(-1830000)))
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected user-not-in-group code to map to not found, got %v", err)
	}

	err = normalizeUserGroupError("remove group member", "research-team", "tempZone", errors.New("received remove group member error: Unknown ErrorCode: -1830000"))
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected user-not-in-group message to map to not found, got %v", err)
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
