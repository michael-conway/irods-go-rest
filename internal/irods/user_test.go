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

func TestUserListMapsAndFiltersUsersByPrefix(t *testing.T) {
	service := newTestUserService(t, newCatalogTestFileSystem())

	users, err := service.ListUsers(context.Background(), bearerRequestContext(), UserListOptions{Prefix: "ali"})
	if err != nil {
		t.Fatalf("ListUsers returned error: %v", err)
	}

	if len(users) != 2 {
		t.Fatalf("expected 2 prefix-matched users, got %+v", users)
	}
	if users[0].Name != "alice" || users[1].Name != "alicia" {
		t.Fatalf("unexpected users: %+v", users)
	}
}

func TestUserListIncludesGroupAdmins(t *testing.T) {
	service := newTestUserService(t, newCatalogTestFileSystem())

	users, err := service.ListUsers(context.Background(), bearerRequestContext(), UserListOptions{
		Type: string(irodstypes.IRODSUserGroupAdmin),
	})
	if err != nil {
		t.Fatalf("ListUsers returned error: %v", err)
	}

	if len(users) != 1 || users[0].Name != "groupadmin" || users[0].Type != string(irodstypes.IRODSUserGroupAdmin) {
		t.Fatalf("unexpected groupadmin users: %+v", users)
	}
}

func TestUserUpdateRequiresRodsAdmin(t *testing.T) {
	service := newTestUserService(t, newCatalogTestFileSystem())

	_, err := service.UpdateUser(context.Background(), bearerRequestContext(), "alice", UserUpdateOptions{
		Type:       string(irodstypes.IRODSUserRodsAdmin),
		ChangeType: true,
	}, UserMutationOptions{})
	if !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("expected permission denied, got %v", err)
	}
}

func TestUserUpdateRejectsGroupAdminTypeChange(t *testing.T) {
	service := newTestUserService(t, newCatalogTestFileSystem())

	_, err := service.UpdateUser(context.Background(), groupAdminRequestContext(), "groupadmin", UserUpdateOptions{
		Type:       string(irodstypes.IRODSUserRodsUser),
		ChangeType: true,
	}, UserMutationOptions{})
	if !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("expected permission denied, got %v", err)
	}
}

func TestUserUpdateReconcileRejectsGroupAdminTypeChange(t *testing.T) {
	service := newTestUserService(t, newCatalogTestFileSystem())

	_, err := service.UpdateUser(context.Background(), groupAdminRequestContext(), "groupadmin", UserUpdateOptions{
		Type:       string(irodstypes.IRODSUserRodsUser),
		ChangeType: true,
	}, UserMutationOptions{Reconcile: true})
	if !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("expected permission denied, got %v", err)
	}
}

func TestUserUpdateChangesTypeForRodsAdmin(t *testing.T) {
	service := newTestUserService(t, newCatalogTestFileSystem())

	updated, err := service.UpdateUser(context.Background(), rodsAdminRequestContext(), "alice", UserUpdateOptions{
		Type:       string(irodstypes.IRODSUserRodsAdmin),
		ChangeType: true,
	}, UserMutationOptions{})
	if err != nil {
		t.Fatalf("UpdateUser returned error: %v", err)
	}
	if updated.Name != "alice" || updated.Type != string(irodstypes.IRODSUserRodsAdmin) {
		t.Fatalf("unexpected updated user: %+v", updated)
	}
}

func TestUserUpdateChangesTypeToGroupAdminForRodsAdmin(t *testing.T) {
	service := newTestUserService(t, newCatalogTestFileSystem())

	updated, err := service.UpdateUser(context.Background(), rodsAdminRequestContext(), "alice", UserUpdateOptions{
		Type:       string(irodstypes.IRODSUserGroupAdmin),
		ChangeType: true,
	}, UserMutationOptions{})
	if err != nil {
		t.Fatalf("UpdateUser returned error: %v", err)
	}
	if updated.Name != "alice" || updated.Type != string(irodstypes.IRODSUserGroupAdmin) {
		t.Fatalf("unexpected updated user: %+v", updated)
	}
}

func TestUserCreateRequiresRodsAdminOrGroupAdmin(t *testing.T) {
	service := newTestUserService(t, newCatalogTestFileSystem())

	_, err := service.CreateUser(context.Background(), bearerRequestContext(), "charlie", UserCreateOptions{
		Type: string(irodstypes.IRODSUserRodsUser),
	}, UserMutationOptions{})
	if !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("expected permission denied, got %v", err)
	}
}

func TestUserCreateRejectsLeakedProxyAdminPrincipal(t *testing.T) {
	filesystem := &adminLeakingCatalogFileSystem{catalogTestFileSystem: newCatalogTestFileSystem()}
	service := newTestUserServiceWithCatalogFileSystem(t, filesystem)

	_, err := service.CreateUser(context.Background(), bearerRequestContext(), "charlie", UserCreateOptions{
		Type: string(irodstypes.IRODSUserRodsUser),
	}, UserMutationOptions{})
	if !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("expected permission denied, got %v", err)
	}
}

func TestUserCreateAllowsGroupAdminToCreateRodsUser(t *testing.T) {
	service := newTestUserService(t, newCatalogTestFileSystem())

	created, err := service.CreateUser(context.Background(), groupAdminRequestContext(), "charlie", UserCreateOptions{
		Type:     string(irodstypes.IRODSUserRodsUser),
		Password: "initial-pass",
	}, UserMutationOptions{})
	if err != nil {
		t.Fatalf("CreateUser returned error: %v", err)
	}
	if created.Name != "charlie" || created.Type != string(irodstypes.IRODSUserRodsUser) {
		t.Fatalf("unexpected created user: %+v", created)
	}
}

func TestUserCreateRejectsGroupAdminProtectedTypes(t *testing.T) {
	service := newTestUserService(t, newCatalogTestFileSystem())

	for _, userType := range []irodstypes.IRODSUserType{irodstypes.IRODSUserGroupAdmin, irodstypes.IRODSUserRodsAdmin} {
		_, err := service.CreateUser(context.Background(), groupAdminRequestContext(), "charlie", UserCreateOptions{
			Type: string(userType),
		}, UserMutationOptions{})
		if !errors.Is(err, ErrPermissionDenied) {
			t.Fatalf("expected permission denied for %s, got %v", userType, err)
		}
	}
}

func TestUserCreateReconcileRejectsGroupAdmin(t *testing.T) {
	service := newTestUserService(t, newCatalogTestFileSystem())

	_, err := service.CreateUser(context.Background(), groupAdminRequestContext(), "charlie", UserCreateOptions{
		Type: string(irodstypes.IRODSUserRodsUser),
	}, UserMutationOptions{Reconcile: true})
	if !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("expected permission denied, got %v", err)
	}
}

func TestUserDeleteRejectsGroupAdmin(t *testing.T) {
	service := newTestUserService(t, newCatalogTestFileSystem())

	err := service.DeleteUser(context.Background(), groupAdminRequestContext(), "alice", "tempZone", UserMutationOptions{})
	if !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("expected permission denied, got %v", err)
	}
}

func TestUserCreateReconcileExistingMatchingType(t *testing.T) {
	service := newTestUserService(t, newCatalogTestFileSystem())

	_, strictErr := service.CreateUser(context.Background(), rodsAdminRequestContext(), "alice", UserCreateOptions{
		Type: string(irodstypes.IRODSUserRodsUser),
	}, UserMutationOptions{})
	if !errors.Is(strictErr, ErrConflict) {
		t.Fatalf("expected strict duplicate create conflict, got %v", strictErr)
	}

	user, err := service.CreateUser(context.Background(), rodsAdminRequestContext(), "alice", UserCreateOptions{
		Type: string(irodstypes.IRODSUserRodsUser),
	}, UserMutationOptions{Reconcile: true})
	if err != nil {
		t.Fatalf("CreateUser reconcile returned error: %v", err)
	}
	if user.Name != "alice" || user.Type != string(irodstypes.IRODSUserRodsUser) {
		t.Fatalf("unexpected reconciled user: %+v", user)
	}
}

func TestUserCreateReconcileMarksRequestSourceMetadata(t *testing.T) {
	filesystem := newCatalogTestFileSystem()
	service := newTestUserService(t, filesystem)
	requestContext := rodsAdminRequestContext()
	requestContext.RequestSource = "external-authz-admin"

	_, err := service.CreateUser(context.Background(), requestContext, "charlie", UserCreateOptions{
		Type: string(irodstypes.IRODSUserRodsUser),
	}, UserMutationOptions{Reconcile: true})
	if err != nil {
		t.Fatalf("CreateUser reconcile returned error: %v", err)
	}

	if !filesystem.hasUserMetadata("charlie", "tempZone", usersyncext.AVUAttributeManaged, usersyncext.AVUValueTrue, "external-authz-admin") {
		t.Fatalf("expected managed source metadata for charlie")
	}
	if !filesystem.hasUserMetadata("charlie", "tempZone", usersyncext.AVUAttributeSource, "external-authz-admin", "") {
		t.Fatalf("expected source metadata for charlie")
	}
}

func TestUserCreateReconcileRejectsTypeMismatch(t *testing.T) {
	service := newTestUserService(t, newCatalogTestFileSystem())

	_, err := service.CreateUser(context.Background(), rodsAdminRequestContext(), "alice", UserCreateOptions{
		Type: string(irodstypes.IRODSUserRodsAdmin),
	}, UserMutationOptions{Reconcile: true})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("expected reconcile type mismatch conflict, got %v", err)
	}
}

func TestUserUpdateReconcileCreatesMissingUserWithType(t *testing.T) {
	service := newTestUserService(t, newCatalogTestFileSystem())

	_, strictErr := service.UpdateUser(context.Background(), rodsAdminRequestContext(), "charlie", UserUpdateOptions{
		Type:       string(irodstypes.IRODSUserRodsUser),
		ChangeType: true,
	}, UserMutationOptions{})
	if !errors.Is(strictErr, ErrNotFound) {
		t.Fatalf("expected strict update not found, got %v", strictErr)
	}

	user, err := service.UpdateUser(context.Background(), rodsAdminRequestContext(), "charlie", UserUpdateOptions{
		Type:       string(irodstypes.IRODSUserRodsUser),
		ChangeType: true,
	}, UserMutationOptions{Reconcile: true})
	if err != nil {
		t.Fatalf("UpdateUser reconcile returned error: %v", err)
	}
	if user.Name != "charlie" || user.Type != string(irodstypes.IRODSUserRodsUser) {
		t.Fatalf("unexpected reconciled user: %+v", user)
	}
}

func TestUserUpdateReconcileWithoutTypeDoesNotCreate(t *testing.T) {
	service := newTestUserService(t, newCatalogTestFileSystem())

	_, err := service.UpdateUser(context.Background(), rodsAdminRequestContext(), "charlie", UserUpdateOptions{
		Password:       "secret",
		ChangePassword: true,
	}, UserMutationOptions{Reconcile: true})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected reconcile update without type to stay not found, got %v", err)
	}
}

func TestUserDeleteReconcileMissingUser(t *testing.T) {
	service := newTestUserService(t, newCatalogTestFileSystem())

	strictErr := service.DeleteUser(context.Background(), rodsAdminRequestContext(), "missing-user", "tempZone", UserMutationOptions{})
	if !errors.Is(strictErr, ErrNotFound) {
		t.Fatalf("expected strict delete not found, got %v", strictErr)
	}

	err := service.DeleteUser(context.Background(), rodsAdminRequestContext(), "missing-user", "tempZone", UserMutationOptions{Reconcile: true})
	if err != nil {
		t.Fatalf("DeleteUser reconcile returned error: %v", err)
	}
}

func TestNormalizeUserErrorMapsCatalogAlreadyHasItemToConflict(t *testing.T) {
	err := normalizeUserError("create user", "alice", "tempZone", irodstypes.NewIRODSError(irodscommon.CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME))
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("expected catalog already-has-item code to map to conflict, got %v", err)
	}

	err = normalizeUserError("create user", "alice", "tempZone", errors.New("received create user error: CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME"))
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("expected catalog already-has-item message to map to conflict, got %v", err)
	}
}

func newTestUserService(t *testing.T, filesystem *catalogTestFileSystem) UserService {
	t.Helper()
	return newTestUserServiceWithCatalogFileSystem(t, filesystem)
}

func newTestUserServiceWithCatalogFileSystem(t *testing.T, filesystem CatalogFileSystem) UserService {
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

	return NewUserServiceWithFactory(cfg, func(_ *irodstypes.IRODSAccount, _ string) (CatalogFileSystem, error) {
		return filesystem, nil
	})
}

func rodsAdminRequestContext() *RequestContext {
	return &RequestContext{
		AuthScheme: "basic",
		Username:   "rods",
	}
}

func groupAdminRequestContext() *RequestContext {
	return &RequestContext{
		AuthScheme: "basic",
		Username:   "groupadmin",
	}
}
