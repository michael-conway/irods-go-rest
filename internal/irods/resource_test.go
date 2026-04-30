package irods

import (
	"context"
	"testing"
	"time"

	irodstypes "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/michael-conway/irods-go-rest/internal/config"
)

func TestResourceListMapsIRODSResources(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	filesystem := newCatalogTestFileSystem()
	filesystem.resources = []*irodstypes.IRODSResource{{
		RescID:     77,
		Name:       "demoResc",
		Zone:       "tempZone",
		Type:       "unixfilesystem",
		Class:      "cache",
		Location:   "irods.example.org",
		Path:       "/var/lib/irods/Vault",
		Context:    "",
		CreateTime: now,
		ModifyTime: now,
	}}
	cfg := config.RestConfig{
		IrodsZone:            "tempZone",
		IrodsHost:            "irods.local",
		IrodsPort:            1247,
		IrodsAuthScheme:      "native",
		IrodsAdminUser:       "rods",
		IrodsAdminPassword:   "rods",
		IrodsDefaultResource: "demoResc",
	}
	service := NewResourceServiceWithFactory(cfg, func(_ *irodstypes.IRODSAccount, _ string) (CatalogFileSystem, error) {
		return filesystem, nil
	})

	resources, err := service.ListResources(context.Background(), bearerRequestContext(), "top")
	if err != nil {
		t.Fatalf("ListResources returned error: %v", err)
	}
	if len(resources) != 1 {
		t.Fatalf("expected 1 resource, got %d", len(resources))
	}
	if resources[0].Name != "demoResc" || resources[0].Zone != "tempZone" {
		t.Fatalf("unexpected resource mapping: %+v", resources[0])
	}
	if resources[0].CreatedAt == nil || resources[0].UpdatedAt == nil {
		t.Fatalf("expected timestamps to be populated: %+v", resources[0])
	}
	if resources[0].Links == nil || resources[0].Links.Self == nil || resources[0].Links.Self.Href != "/api/v1/resource/demoResc" {
		t.Fatalf("expected self link to be populated: %+v", resources[0])
	}
}
