//go:build integration
// +build integration

package irods

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"
)

func TestCatalogGetPathCollectionIntegration(t *testing.T) {
	service := newIntegrationCatalogService(t)
	fixture := newCatalogIntegrationFixture(t)

	entry, err := service.GetPath(context.Background(), integrationCatalogRequestContext(t), fixture.rootPath, PathLookupOptions{})
	if err != nil {
		t.Fatalf("GetPath returned error: %v", err)
	}

	if entry.Path != fixture.rootPath {
		t.Fatalf("expected path %q, got %q", fixture.rootPath, entry.Path)
	}
	if entry.Kind != "collection" {
		t.Fatalf("expected collection kind, got %q", entry.Kind)
	}
	if !entry.HasChildren {
		t.Fatal("expected collection to have children")
	}
	if entry.ChildCount < 2 {
		t.Fatalf("expected childCount >= 2, got %d", entry.ChildCount)
	}
	if got := entry.Metadata["catalog.integration.collection"]; got != "root" {
		t.Fatalf("expected collection metadata value %q, got %q", "root", got)
	}
}

func TestCatalogGetPathDataObjectIntegration(t *testing.T) {
	service := newIntegrationCatalogService(t)
	fixture := newCatalogIntegrationFixture(t)

	entry, err := service.GetPath(context.Background(), integrationCatalogRequestContext(t), fixture.objectPath, PathLookupOptions{})
	if err != nil {
		t.Fatalf("GetPath returned error: %v", err)
	}

	if entry.Path != fixture.objectPath {
		t.Fatalf("expected path %q, got %q", fixture.objectPath, entry.Path)
	}
	if entry.Kind != "data_object" {
		t.Fatalf("expected data_object kind, got %q", entry.Kind)
	}
	if entry.Size != int64(len(fixture.objectContent)) {
		t.Fatalf("expected size %d, got %d", len(fixture.objectContent), entry.Size)
	}
	if entry.Resource == "" {
		t.Fatal("expected resource to be populated")
	}
	if got := entry.Metadata["catalog.integration.object"]; got != "payload" {
		t.Fatalf("expected object metadata value %q, got %q", "payload", got)
	}
}

func TestCatalogGetPathChildrenIntegration(t *testing.T) {
	service := newIntegrationCatalogService(t)
	fixture := newCatalogIntegrationFixture(t)

	children, err := service.GetPathChildren(context.Background(), integrationCatalogRequestContext(t), fixture.rootPath)
	if err != nil {
		t.Fatalf("GetPathChildren returned error: %v", err)
	}

	if len(children) < 2 {
		t.Fatalf("expected at least 2 children, got %d", len(children))
	}

	var foundObject bool
	var foundCollection bool
	for _, child := range children {
		switch child.Path {
		case fixture.objectPath:
			foundObject = child.Kind == "data_object"
		case fixture.childCollectionPath:
			foundCollection = child.Kind == "collection"
		}
	}

	if !foundObject {
		t.Fatalf("expected object child %q in result", fixture.objectPath)
	}
	if !foundCollection {
		t.Fatalf("expected collection child %q in result", fixture.childCollectionPath)
	}
}

func TestCatalogGetObjectContentByPathIntegration(t *testing.T) {
	service := newIntegrationCatalogService(t)
	fixture := newCatalogIntegrationFixture(t)

	content, err := service.GetObjectContentByPath(context.Background(), integrationCatalogRequestContext(t), fixture.objectPath)
	if err != nil {
		t.Fatalf("GetObjectContentByPath returned error: %v", err)
	}
	defer func() {
		if content.Reader != nil {
			_ = content.Reader.Close()
		}
	}()

	if content.Path != fixture.objectPath {
		t.Fatalf("expected path %q, got %q", fixture.objectPath, content.Path)
	}
	if content.Size != int64(len(fixture.objectContent)) {
		t.Fatalf("expected size %d, got %d", len(fixture.objectContent), content.Size)
	}
	if content.ContentType != "text/plain; charset=utf-8" {
		t.Fatalf("expected content type %q, got %q", "text/plain; charset=utf-8", content.ContentType)
	}

	buffer := make([]byte, len(fixture.objectContent))
	n, err := content.Reader.ReadAt(buffer, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("ReadAt returned error: %v", err)
	}

	if got := buffer[:n]; !bytes.Equal(got, fixture.objectContent[:n]) {
		t.Fatalf("expected content %q, got %q", string(fixture.objectContent[:n]), string(got))
	}
}

func TestCatalogGetPathNormalizesNotFoundIntegration(t *testing.T) {
	service := newIntegrationCatalogService(t)

	_, err := service.GetPath(context.Background(), integrationCatalogRequestContext(t), integrationMissingPath(t), PathLookupOptions{})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestCatalogGetPathProxyIntegration(t *testing.T) {
	service := newIntegrationCatalogService(t)
	fixture := newCatalogIntegrationFixture(t)

	entry, err := service.GetPath(context.Background(), integrationBearerRequestContext(t), fixture.objectPath, PathLookupOptions{})
	if err != nil {
		t.Fatalf("GetPath with bearer proxy context returned error: %v", err)
	}

	if entry.Path != fixture.objectPath {
		t.Fatalf("expected path %q, got %q", fixture.objectPath, entry.Path)
	}
	if entry.Kind != "data_object" {
		t.Fatalf("expected data_object kind, got %q", entry.Kind)
	}
}

type catalogIntegrationFixture struct {
	rootPath            string
	childCollectionPath string
	objectPath          string
	objectContent       []byte
}

func newCatalogIntegrationFixture(t *testing.T) *catalogIntegrationFixture {
	t.Helper()

	filesystem := newIntegrationIRODSFilesystem(t)

	rootPath := fmt.Sprintf(
		"/%s/home/%s/catalog-integration-%d",
		integrationIRODSZone(t),
		integrationBasicUsername(t),
		time.Now().UnixNano(),
	)
	if err := filesystem.MakeDir(rootPath, true); err != nil {
		filesystem.Release()
		t.Fatalf("make fixture root %q: %v", rootPath, err)
	}

	t.Cleanup(func() {
		defer filesystem.Release()
		if err := filesystem.RemoveDir(rootPath, true, true); err != nil && filesystem.Exists(rootPath) {
			t.Errorf("cleanup dir %q: %v", rootPath, err)
		}
	})

	childCollectionPath := rootPath + "/nested"
	if err := filesystem.MakeDir(childCollectionPath, true); err != nil {
		t.Fatalf("make child collection %q: %v", childCollectionPath, err)
	}

	objectPath := rootPath + "/fixture.txt"
	objectContent := []byte("catalog integration payload\n")
	if _, err := filesystem.UploadFileFromBuffer(bytes.NewBuffer(objectContent), objectPath, "", false, true, nil); err != nil {
		t.Fatalf("upload object %q: %v", objectPath, err)
	}

	if err := filesystem.AddMetadata(rootPath, "catalog.integration.collection", "root", ""); err != nil {
		t.Fatalf("add collection metadata: %v", err)
	}
	if err := filesystem.AddMetadata(objectPath, "catalog.integration.object", "payload", ""); err != nil {
		t.Fatalf("add object metadata: %v", err)
	}

	return &catalogIntegrationFixture{
		rootPath:            rootPath,
		childCollectionPath: childCollectionPath,
		objectPath:          objectPath,
		objectContent:       objectContent,
	}
}

func integrationMissingPath(t *testing.T) string {
	return fmt.Sprintf(
		"/%s/home/%s/catalog-integration-missing",
		integrationIRODSZone(t),
		integrationBasicUsername(t),
	)
}
