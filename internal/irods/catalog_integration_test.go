//go:build integration
// +build integration

package irods

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"path/filepath"
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
	expectedMimeType := mime.TypeByExtension(filepath.Ext(fixture.objectPath))
	if expectedMimeType == "" {
		expectedMimeType = "application/octet-stream"
	}
	if entry.MimeType != expectedMimeType {
		t.Fatalf("expected mime type %q, got %q", expectedMimeType, entry.MimeType)
	}
	if got := entry.Metadata["catalog.integration.object"]; got != "payload" {
		t.Fatalf("expected object metadata value %q, got %q", "payload", got)
	}
}

func TestCatalogPathChecksumIntegration(t *testing.T) {
	service := newIntegrationCatalogService(t)
	fixture := newCatalogIntegrationFixture(t)

	initial, err := service.GetPathChecksum(context.Background(), integrationCatalogRequestContext(t), fixture.objectPath)
	if err != nil {
		t.Fatalf("GetPathChecksum returned error: %v", err)
	}
	if initial.Checksum != "" || initial.Type != "" {
		t.Fatalf("expected empty checksum before compute, got %+v", initial)
	}

	computed, err := service.ComputePathChecksum(context.Background(), integrationCatalogRequestContext(t), fixture.objectPath)
	if err != nil {
		t.Fatalf("ComputePathChecksum returned error: %v", err)
	}
	if computed.Checksum == "" {
		t.Fatal("expected computed checksum to be populated")
	}
	if computed.Type == "" {
		t.Fatal("expected computed checksum type to be populated")
	}

	current, err := service.GetPathChecksum(context.Background(), integrationCatalogRequestContext(t), fixture.objectPath)
	if err != nil {
		t.Fatalf("GetPathChecksum after compute returned error: %v", err)
	}
	if current.Checksum != computed.Checksum || current.Type != computed.Type {
		t.Fatalf("expected current checksum %+v to match computed %+v", current, computed)
	}

	entry, err := service.GetPath(context.Background(), integrationCatalogRequestContext(t), fixture.objectPath, PathLookupOptions{})
	if err != nil {
		t.Fatalf("GetPath after compute returned error: %v", err)
	}
	if entry.Checksum == nil || entry.Checksum.Checksum != computed.Checksum {
		t.Fatalf("expected path checksum %q after compute, got %+v", computed.Checksum, entry.Checksum)
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

func TestCatalogPathMetadataAddDeleteIntegration(t *testing.T) {
	service := newIntegrationCatalogService(t)
	fixture := newCatalogIntegrationFixture(t)

	created, err := service.AddPathMetadata(context.Background(), integrationCatalogRequestContext(t), fixture.objectPath, "catalog.integration.added", "present", "test")
	if err != nil {
		t.Fatalf("AddPathMetadata returned error: %v", err)
	}
	if created.ID == "" {
		t.Fatal("expected created AVU id to be populated")
	}

	metadata, err := service.GetPathMetadata(context.Background(), integrationCatalogRequestContext(t), fixture.objectPath)
	if err != nil {
		t.Fatalf("GetPathMetadata returned error: %v", err)
	}

	found := false
	for _, avu := range metadata {
		if avu.ID == created.ID && avu.Attrib == "catalog.integration.added" && avu.Value == "present" && avu.Unit == "test" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected created AVU %q in metadata list", created.ID)
	}

	if err := service.DeletePathMetadata(context.Background(), integrationCatalogRequestContext(t), fixture.objectPath, created.ID); err != nil {
		t.Fatalf("DeletePathMetadata returned error: %v", err)
	}

	metadata, err = service.GetPathMetadata(context.Background(), integrationCatalogRequestContext(t), fixture.objectPath)
	if err != nil {
		t.Fatalf("GetPathMetadata after delete returned error: %v", err)
	}
	for _, avu := range metadata {
		if avu.ID == created.ID {
			t.Fatalf("expected AVU %q to be removed", created.ID)
		}
	}
}

func TestCatalogPathMetadataUpdateIntegration(t *testing.T) {
	service := newIntegrationCatalogService(t)
	fixture := newCatalogIntegrationFixture(t)

	created, err := service.AddPathMetadata(context.Background(), integrationCatalogRequestContext(t), fixture.objectPath, "catalog.integration.update", "before", "test")
	if err != nil {
		t.Fatalf("AddPathMetadata returned error: %v", err)
	}

	updated, err := service.UpdatePathMetadata(context.Background(), integrationCatalogRequestContext(t), fixture.objectPath, created.ID, "catalog.integration.update", "after", "test")
	if err != nil {
		t.Fatalf("UpdatePathMetadata returned error: %v", err)
	}
	if updated.Value != "after" {
		t.Fatalf("expected updated AVU value, got %+v", updated)
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
	if content.FileName == "" {
		t.Fatal("expected file name to be populated")
	}
	if content.Checksum != nil {
		t.Fatalf("expected checksum to be absent before explicit compute, got %+v", content.Checksum)
	}
	if content.UpdatedAt == nil {
		t.Fatal("expected updated timestamp to be populated")
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
	if _, err := filesystem.UploadFileFromBuffer(bytes.NewBuffer(objectContent), objectPath, "", false, false, nil); err != nil {
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
