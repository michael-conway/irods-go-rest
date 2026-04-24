package irods

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	irodsfs "github.com/cyverse/go-irodsclient/fs"
	irodscommon "github.com/cyverse/go-irodsclient/irods/common"
	irodstypes "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/michael-conway/irods-go-rest/internal/config"
)

func TestCatalogGetPathCollectionMapsFilesystemEntry(t *testing.T) {
	service := newTestCatalogService(t, newCatalogTestFileSystem())

	entry, err := service.GetPath(context.Background(), bearerRequestContext(), "/tempZone/home/alice/project")
	if err != nil {
		t.Fatalf("GetPath returned error: %v", err)
	}

	if entry.Kind != "collection" {
		t.Fatalf("expected collection kind, got %q", entry.Kind)
	}
	if !entry.HasChildren {
		t.Fatal("expected collection to have children")
	}
	if entry.ChildCount != 2 {
		t.Fatalf("expected child count 2, got %d", entry.ChildCount)
	}
	if got := entry.Metadata["project"]; got != "demo" {
		t.Fatalf("expected metadata mapping, got %q", got)
	}
}

func TestCatalogGetPathDataObjectMapsFilesystemEntry(t *testing.T) {
	service := newTestCatalogService(t, newCatalogTestFileSystem())

	entry, err := service.GetPath(context.Background(), bearerRequestContext(), "/tempZone/home/alice/file.txt")
	if err != nil {
		t.Fatalf("GetPath returned error: %v", err)
	}

	if entry.Kind != "data_object" {
		t.Fatalf("expected data_object kind, got %q", entry.Kind)
	}
	if entry.Size != 21 {
		t.Fatalf("expected size 21, got %d", entry.Size)
	}
	if entry.Resource != "demoResc" {
		t.Fatalf("expected resource demoResc, got %q", entry.Resource)
	}
	if entry.Checksum == "" {
		t.Fatal("expected checksum to be populated")
	}
	if got := entry.Metadata["source"]; got != "unit-test" {
		t.Fatalf("expected metadata mapping, got %q", got)
	}
}

func TestCatalogGetPathChildrenMapsChildEntries(t *testing.T) {
	service := newTestCatalogService(t, newCatalogTestFileSystem())

	children, err := service.GetPathChildren(context.Background(), bearerRequestContext(), "/tempZone/home/alice/project")
	if err != nil {
		t.Fatalf("GetPathChildren returned error: %v", err)
	}

	if len(children) != 2 {
		t.Fatalf("expected 2 children, got %d", len(children))
	}
	if children[0].Kind != "data_object" {
		t.Fatalf("expected first child to be data_object, got %q", children[0].Kind)
	}
	if children[1].Kind != "collection" {
		t.Fatalf("expected second child to be collection, got %q", children[1].Kind)
	}
}

func TestCatalogGetPathNormalizesNotFound(t *testing.T) {
	service := newTestCatalogService(t, newCatalogTestFileSystem())

	_, err := service.GetPath(context.Background(), bearerRequestContext(), "/tempZone/home/alice/missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestCatalogGetPathNormalizesPermissionDenied(t *testing.T) {
	service := newTestCatalogService(t, newCatalogTestFileSystem())

	_, err := service.GetPath(context.Background(), bearerRequestContext(), "/tempZone/home/alice/forbidden")
	if !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("expected ErrPermissionDenied, got %v", err)
	}
}

func TestCatalogGetObjectContentByPathReturnsReader(t *testing.T) {
	filesystem := newCatalogTestFileSystem()
	service := newTestCatalogService(t, filesystem)

	content, err := service.GetObjectContentByPath(context.Background(), bearerRequestContext(), "/tempZone/home/alice/file.txt")
	if err != nil {
		t.Fatalf("GetObjectContentByPath returned error: %v", err)
	}

	buffer := make([]byte, 5)
	n, err := content.Reader.ReadAt(buffer, 6)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("ReadAt returned error: %v", err)
	}
	if got := string(buffer[:n]); got != "conte" {
		t.Fatalf("expected ranged content %q, got %q", "conte", got)
	}

	if err := content.Reader.Close(); err != nil {
		t.Fatalf("reader Close returned error: %v", err)
	}
	if !filesystem.released {
		t.Fatal("expected filesystem to be released when content reader closes")
	}
}

func TestCatalogGetObjectContentByPathRejectsCollections(t *testing.T) {
	service := newTestCatalogService(t, newCatalogTestFileSystem())

	_, err := service.GetObjectContentByPath(context.Background(), bearerRequestContext(), "/tempZone/home/alice/project")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for collection read, got %v", err)
	}
}

func newTestCatalogService(t *testing.T, filesystem *catalogTestFileSystem) CatalogService {
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

	return NewCatalogServiceWithFactory(cfg, func(_ *irodstypes.IRODSAccount, _ string) (CatalogFileSystem, error) {
		return filesystem, nil
	})
}

func bearerRequestContext() *RequestContext {
	return &RequestContext{
		AuthScheme: "bearer",
		Username:   "alice",
	}
}

type catalogTestFileSystem struct {
	entriesByPath  map[string]*irodsfs.Entry
	childrenByPath map[string][]*irodsfs.Entry
	metadataByPath map[string][]*irodstypes.IRODSMeta
	contentByPath  map[string][]byte
	released       bool
}

func newCatalogTestFileSystem() *catalogTestFileSystem {
	now := time.Unix(1_700_000_000, 0)

	project := &irodsfs.Entry{
		ID:         100,
		Type:       irodsfs.DirectoryEntry,
		Name:       "project",
		Path:       "/tempZone/home/alice/project",
		CreateTime: now,
		ModifyTime: now,
	}
	file := &irodsfs.Entry{
		ID:                101,
		Type:              irodsfs.FileEntry,
		Name:              "file.txt",
		Path:              "/tempZone/home/alice/file.txt",
		Size:              21,
		CheckSumAlgorithm: irodstypes.ChecksumAlgorithmSHA256,
		CheckSum:          []byte("abc123"),
		IRODSReplicas: []irodstypes.IRODSReplica{{
			ResourceName: "demoResc",
		}},
		CreateTime: now,
		ModifyTime: now,
	}
	child := &irodsfs.Entry{
		ID:         102,
		Type:       irodsfs.FileEntry,
		Name:       "child.txt",
		Path:       "/tempZone/home/alice/project/child.txt",
		Size:       10,
		CreateTime: now,
		ModifyTime: now,
	}
	nested := &irodsfs.Entry{
		ID:         103,
		Type:       irodsfs.DirectoryEntry,
		Name:       "nested",
		Path:       "/tempZone/home/alice/project/nested",
		CreateTime: now,
		ModifyTime: now,
	}

	return &catalogTestFileSystem{
		entriesByPath: map[string]*irodsfs.Entry{
			project.Path: project,
			file.Path:    file,
			child.Path:   child,
			nested.Path:  nested,
		},
		childrenByPath: map[string][]*irodsfs.Entry{
			project.Path: {child, nested},
		},
		metadataByPath: map[string][]*irodstypes.IRODSMeta{
			project.Path: {{
				Name:  "project",
				Value: "demo",
			}},
			file.Path: {{
				Name:  "source",
				Value: "unit-test",
			}},
		},
		contentByPath: map[string][]byte{
			file.Path: []byte("hello content payload"),
		},
	}
}

func (f *catalogTestFileSystem) Stat(irodsPath string) (*irodsfs.Entry, error) {
	if irodsPath == "/tempZone/home/alice/forbidden" {
		return nil, irodstypes.NewIRODSError(irodscommon.CAT_NO_ACCESS_PERMISSION)
	}

	entry, ok := f.entriesByPath[irodsPath]
	if !ok {
		return nil, irodstypes.NewFileNotFoundError(irodsPath)
	}
	return entry, nil
}

func (f *catalogTestFileSystem) List(irodsPath string) ([]*irodsfs.Entry, error) {
	if irodsPath == "/tempZone/home/alice/forbidden" {
		return nil, irodstypes.NewIRODSError(irodscommon.CAT_NO_ACCESS_PERMISSION)
	}

	return f.childrenByPath[irodsPath], nil
}

func (f *catalogTestFileSystem) ListMetadata(irodsPath string) ([]*irodstypes.IRODSMeta, error) {
	if irodsPath == "/tempZone/home/alice/forbidden" {
		return nil, irodstypes.NewIRODSError(irodscommon.CAT_NO_ACCESS_PERMISSION)
	}

	return f.metadataByPath[irodsPath], nil
}

func (f *catalogTestFileSystem) OpenFile(irodsPath string, _ string, _ string) (CatalogFileHandle, error) {
	if irodsPath == "/tempZone/home/alice/forbidden" {
		return nil, irodstypes.NewIRODSError(irodscommon.CAT_NO_ACCESS_PERMISSION)
	}

	data, ok := f.contentByPath[irodsPath]
	if !ok {
		return nil, irodstypes.NewFileNotFoundError(irodsPath)
	}

	return &catalogTestFileHandle{reader: bytes.NewReader(data)}, nil
}

func (f *catalogTestFileSystem) Release() {
	f.released = true
}

type catalogTestFileHandle struct {
	reader *bytes.Reader
}

func (f *catalogTestFileHandle) ReadAt(buffer []byte, offset int64) (int, error) {
	return f.reader.ReadAt(buffer, offset)
}

func (f *catalogTestFileHandle) Close() error {
	return nil
}
