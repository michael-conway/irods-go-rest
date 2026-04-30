package irods

import (
	"context"
	"errors"
	"testing"

	irodstypes "github.com/cyverse/go-irodsclient/irods/types"
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

func TestUserUpdateRequiresRodsAdmin(t *testing.T) {
	service := newTestUserService(t, newCatalogTestFileSystem())

	_, err := service.UpdateUser(context.Background(), bearerRequestContext(), "alice", UserUpdateOptions{
		Type:       string(irodstypes.IRODSUserRodsAdmin),
		ChangeType: true,
	})
	if !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("expected permission denied, got %v", err)
	}
}

func TestUserUpdateChangesTypeForRodsAdmin(t *testing.T) {
	service := newTestUserService(t, newCatalogTestFileSystem())

	updated, err := service.UpdateUser(context.Background(), rodsAdminRequestContext(), "alice", UserUpdateOptions{
		Type:       string(irodstypes.IRODSUserRodsAdmin),
		ChangeType: true,
	})
	if err != nil {
		t.Fatalf("UpdateUser returned error: %v", err)
	}
	if updated.Name != "alice" || updated.Type != string(irodstypes.IRODSUserRodsAdmin) {
		t.Fatalf("unexpected updated user: %+v", updated)
	}
}

func TestUserCreateRequiresAdminOrGroupAdmin(t *testing.T) {
	service := newTestUserService(t, newCatalogTestFileSystem())

	_, err := service.CreateUser(context.Background(), bearerRequestContext(), "charlie", UserCreateOptions{
		Type: string(irodstypes.IRODSUserRodsUser),
	})
	if !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("expected permission denied, got %v", err)
	}
}

func TestUserCreateSucceedsForGroupAdmin(t *testing.T) {
	service := newTestUserService(t, newCatalogTestFileSystem())

	created, err := service.CreateUser(context.Background(), groupAdminRequestContext(), "charlie", UserCreateOptions{
		Type: string(irodstypes.IRODSUserRodsUser),
	})
	if err != nil {
		t.Fatalf("CreateUser returned error: %v", err)
	}
	if created.Name != "charlie" || created.Type != string(irodstypes.IRODSUserRodsUser) {
		t.Fatalf("unexpected created user: %+v", created)
	}
}

func TestUserDeleteSucceedsForGroupAdmin(t *testing.T) {
	service := newTestUserService(t, newCatalogTestFileSystem())

	err := service.DeleteUser(context.Background(), groupAdminRequestContext(), "alice", "tempZone")
	if err != nil {
		t.Fatalf("DeleteUser returned error: %v", err)
	}
}

func newTestUserService(t *testing.T, filesystem *catalogTestFileSystem) UserService {
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
