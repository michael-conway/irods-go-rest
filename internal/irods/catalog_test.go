package irods

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path"
	"strings"
	"testing"
	"time"

	irodsfs "github.com/cyverse/go-irodsclient/fs"
	irodscommon "github.com/cyverse/go-irodsclient/irods/common"
	irodstypes "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/michael-conway/irods-go-rest/internal/config"
)

func TestCatalogGetPathCollectionMapsFilesystemEntry(t *testing.T) {
	service := newTestCatalogService(t, newCatalogTestFileSystem())

	entry, err := service.GetPath(context.Background(), bearerRequestContext(), "/tempZone/home/alice/project", PathLookupOptions{})
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
	if entry.CreatedAt == nil || entry.UpdatedAt == nil {
		t.Fatal("expected collection timestamps to be populated")
	}
	if len(entry.Replicas) != 0 {
		t.Fatalf("expected replicas to be omitted without verbose flag, got %d", len(entry.Replicas))
	}
}

func TestCatalogGetPathDataObjectMapsFilesystemEntry(t *testing.T) {
	service := newTestCatalogService(t, newCatalogTestFileSystem())

	entry, err := service.GetPath(context.Background(), bearerRequestContext(), "/tempZone/home/alice/file.txt", PathLookupOptions{})
	if err != nil {
		t.Fatalf("GetPath returned error: %v", err)
	}

	if entry.Kind != "data_object" {
		t.Fatalf("expected data_object kind, got %q", entry.Kind)
	}
	if entry.Size != 21 {
		t.Fatalf("expected size 21, got %d", entry.Size)
	}
	if entry.DisplaySize != "21 B" {
		t.Fatalf("expected display size 21 B, got %q", entry.DisplaySize)
	}
	if entry.Resource != "demoResc" {
		t.Fatalf("expected resource demoResc, got %q", entry.Resource)
	}
	if entry.Checksum == nil || entry.Checksum.Checksum == "" {
		t.Fatal("expected checksum to be populated")
	}
	if entry.Checksum.Type != "sha2" {
		t.Fatalf("expected checksum type sha2, got %+v", entry.Checksum)
	}
	if entry.MimeType != "text/plain; charset=utf-8" {
		t.Fatalf("expected mime type text/plain; charset=utf-8, got %q", entry.MimeType)
	}
	if got := entry.Metadata["source"]; got != "unit-test" {
		t.Fatalf("expected metadata mapping, got %q", got)
	}
	if entry.CreatedAt == nil || entry.UpdatedAt == nil {
		t.Fatal("expected data object timestamps to be populated")
	}
	if len(entry.Replicas) != 0 {
		t.Fatalf("expected replicas to be omitted without verbose flag, got %d", len(entry.Replicas))
	}
}

func TestCatalogGetPathVerboseMapsReplicaLongFields(t *testing.T) {
	service := newTestCatalogService(t, newCatalogTestFileSystem())

	entry, err := service.GetPath(context.Background(), bearerRequestContext(), "/tempZone/home/alice/file.txt", PathLookupOptions{VerboseLevel: 1})
	if err != nil {
		t.Fatalf("GetPath returned error: %v", err)
	}

	if len(entry.Replicas) != 1 {
		t.Fatalf("expected 1 replica, got %d", len(entry.Replicas))
	}

	replica := entry.Replicas[0]
	if replica.Owner != "alice" || replica.ResourceHierarchy != "demoResc" {
		t.Fatalf("unexpected replica mapping: %+v", replica)
	}
	if replica.StatusSymbol != "&" || replica.StatusDescription != "good" {
		t.Fatalf("expected good replica status mapping, got %+v", replica)
	}
	if replica.Checksum != "" || replica.PhysicalPath != "" || replica.DataType != "" {
		t.Fatalf("expected very-long fields to be omitted at verbose=1, got %+v", replica)
	}
}

func TestCatalogGetPathVerboseMapsReplicaVeryLongFields(t *testing.T) {
	service := newTestCatalogService(t, newCatalogTestFileSystem())

	entry, err := service.GetPath(context.Background(), bearerRequestContext(), "/tempZone/home/alice/file.txt", PathLookupOptions{VerboseLevel: 2})
	if err != nil {
		t.Fatalf("GetPath returned error: %v", err)
	}

	replica := entry.Replicas[0]
	if replica.Checksum != "sha2:YWJjMTIz" {
		t.Fatalf("expected replica checksum, got %q", replica.Checksum)
	}
	if replica.DataType != "generic" {
		t.Fatalf("expected replica data type generic, got %q", replica.DataType)
	}
	if replica.PhysicalPath != "/var/lib/irods/Vault/home/alice/file.txt" {
		t.Fatalf("expected replica physical path, got %q", replica.PhysicalPath)
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
	if children[0].DisplaySize != "10 B" {
		t.Fatalf("expected first child display size 10 B, got %q", children[0].DisplaySize)
	}
	if children[0].CreatedAt == nil || children[0].UpdatedAt == nil {
		t.Fatal("expected first child timestamps to be populated")
	}
	if children[1].Kind != "collection" {
		t.Fatalf("expected second child to be collection, got %q", children[1].Kind)
	}
	if children[1].CreatedAt == nil || children[1].UpdatedAt == nil {
		t.Fatal("expected second child timestamps to be populated")
	}
}

func TestCatalogSearchPathChildrenMatchesBasenamePattern(t *testing.T) {
	service := newTestCatalogService(t, newCatalogTestFileSystem())

	result, err := service.SearchPathChildren(context.Background(), bearerRequestContext(), "/tempZone/home/alice/project", PathChildrenListOptions{
		NamePattern:   "child*",
		SearchScope:   PathChildrenSearchScopeChildren,
		CaseSensitive: true,
	})
	if err != nil {
		t.Fatalf("SearchPathChildren returned error: %v", err)
	}

	if result.MatchedCount != 1 || len(result.Children) != 1 {
		t.Fatalf("expected one child match, got matched_count=%d children=%d", result.MatchedCount, len(result.Children))
	}
	if result.Children[0].Path != "/tempZone/home/alice/project/child.txt" {
		t.Fatalf("unexpected child match path: %q", result.Children[0].Path)
	}
}

func TestCatalogSearchPathChildrenSupportsSubtreeAndCaseInsensitive(t *testing.T) {
	filesystem := newCatalogTestFileSystem()
	now := time.Unix(1_700_000_005, 0)
	nestedFile := &irodsfs.Entry{
		ID:         104,
		Type:       irodsfs.FileEntry,
		Name:       "Report.TXT",
		Owner:      "alice",
		Path:       "/tempZone/home/alice/project/nested/Report.TXT",
		Size:       8,
		DataType:   "generic",
		CreateTime: now,
		ModifyTime: now,
	}
	filesystem.entriesByPath[nestedFile.Path] = nestedFile
	filesystem.childrenByPath["/tempZone/home/alice/project/nested"] = []*irodsfs.Entry{nestedFile}
	service := newTestCatalogService(t, filesystem)

	result, err := service.SearchPathChildren(context.Background(), bearerRequestContext(), "/tempZone/home/alice/project", PathChildrenListOptions{
		NamePattern:   "*.txt",
		SearchScope:   PathChildrenSearchScopeSubtree,
		CaseSensitive: false,
		Sort:          "path",
		Order:         "asc",
	})
	if err != nil {
		t.Fatalf("SearchPathChildren returned error: %v", err)
	}

	if result.MatchedCount != 2 || len(result.Children) != 2 {
		t.Fatalf("expected two subtree matches, got matched_count=%d children=%d", result.MatchedCount, len(result.Children))
	}
	if result.Children[0].Path != "/tempZone/home/alice/project/child.txt" || result.Children[1].Path != nestedFile.Path {
		t.Fatalf("unexpected subtree search ordering: %+v", result.Children)
	}
}

func TestCatalogSearchPathChildrenSupportsAbsoluteScope(t *testing.T) {
	filesystem := newCatalogTestFileSystem()
	now := time.Unix(1_700_000_006, 0)
	zoneRoot := &irodsfs.Entry{ID: 900, Type: irodsfs.DirectoryEntry, Name: "tempZone", Path: "/tempZone", CreateTime: now, ModifyTime: now}
	zoneHome := &irodsfs.Entry{ID: 901, Type: irodsfs.DirectoryEntry, Name: "home", Path: "/tempZone/home", CreateTime: now, ModifyTime: now}
	zoneAlice := &irodsfs.Entry{ID: 902, Type: irodsfs.DirectoryEntry, Name: "alice", Path: "/tempZone/home/alice", CreateTime: now, ModifyTime: now}
	zoneFile := &irodsfs.Entry{
		ID:         105,
		Type:       irodsfs.FileEntry,
		Name:       "zone-log.txt",
		Owner:      "alice",
		Path:       "/tempZone/home/alice/zone-log.txt",
		Size:       8,
		DataType:   "generic",
		CreateTime: now,
		ModifyTime: now,
	}
	filesystem.entriesByPath[zoneRoot.Path] = zoneRoot
	filesystem.entriesByPath[zoneHome.Path] = zoneHome
	filesystem.entriesByPath[zoneAlice.Path] = zoneAlice
	filesystem.entriesByPath[zoneFile.Path] = zoneFile
	filesystem.childrenByPath["/tempZone"] = []*irodsfs.Entry{zoneHome}
	filesystem.childrenByPath["/tempZone/home"] = []*irodsfs.Entry{zoneAlice}
	filesystem.childrenByPath["/tempZone/home/alice"] = []*irodsfs.Entry{filesystem.entriesByPath["/tempZone/home/alice/project"], filesystem.entriesByPath["/tempZone/home/alice/file.txt"], zoneFile}
	service := newTestCatalogService(t, filesystem)

	result, err := service.SearchPathChildren(context.Background(), bearerRequestContext(), "/tempZone/home/alice/project", PathChildrenListOptions{
		NamePattern:   "/tempZone/home/alice/zone-*",
		SearchScope:   PathChildrenSearchScopeAbsolute,
		CaseSensitive: true,
	})
	if err != nil {
		t.Fatalf("SearchPathChildren returned error: %v", err)
	}

	if result.MatchedCount != 1 || len(result.Children) != 1 {
		t.Fatalf("expected one absolute match, got matched_count=%d children=%d", result.MatchedCount, len(result.Children))
	}
	if result.Children[0].Path != zoneFile.Path {
		t.Fatalf("unexpected absolute search match path: %q", result.Children[0].Path)
	}
}

func TestCatalogGetPathReplicasReturnsReplicaList(t *testing.T) {
	service := newTestCatalogService(t, newCatalogTestFileSystem())

	replicas, err := service.GetPathReplicas(context.Background(), bearerRequestContext(), "/tempZone/home/alice/file.txt", 2)
	if err != nil {
		t.Fatalf("GetPathReplicas returned error: %v", err)
	}

	if len(replicas) != 1 {
		t.Fatalf("expected 1 replica, got %+v", replicas)
	}
	if replicas[0].ResourceName != "demoResc" {
		t.Fatalf("expected demoResc replica, got %+v", replicas[0])
	}
}

func TestCatalogCreatePathReplicaCreatesReplicaOnResource(t *testing.T) {
	service := newTestCatalogService(t, newCatalogTestFileSystem())

	replicas, err := service.CreatePathReplica(context.Background(), bearerRequestContext(), "/tempZone/home/alice/file.txt", PathReplicaCreateOptions{
		Resource: "archiveResc",
		Update:   true,
	})
	if err != nil {
		t.Fatalf("CreatePathReplica returned error: %v", err)
	}

	if len(replicas) != 2 {
		t.Fatalf("expected 2 replicas after create, got %+v", replicas)
	}
}

func TestCatalogMovePathReplicaMovesReplicaBetweenResources(t *testing.T) {
	service := newTestCatalogService(t, newCatalogTestFileSystem())

	_, err := service.CreatePathReplica(context.Background(), bearerRequestContext(), "/tempZone/home/alice/file.txt", PathReplicaCreateOptions{
		Resource: "cacheResc",
		Update:   true,
	})
	if err != nil {
		t.Fatalf("CreatePathReplica returned error: %v", err)
	}

	replicas, err := service.MovePathReplica(context.Background(), bearerRequestContext(), "/tempZone/home/alice/file.txt", PathReplicaMoveOptions{
		SourceResource:      "cacheResc",
		DestinationResource: "archiveResc",
		Update:              true,
		MinCopies:           1,
	})
	if err != nil {
		t.Fatalf("MovePathReplica returned error: %v", err)
	}

	if len(replicas) != 2 {
		t.Fatalf("expected 2 replicas after move, got %+v", replicas)
	}

	hasSource := false
	hasDestination := false
	for _, replica := range replicas {
		if replica.ResourceName == "cacheResc" {
			hasSource = true
		}
		if replica.ResourceName == "archiveResc" {
			hasDestination = true
		}
	}

	if hasSource {
		t.Fatalf("expected source replica to be trimmed, got %+v", replicas)
	}
	if !hasDestination {
		t.Fatalf("expected destination replica to exist, got %+v", replicas)
	}
}

func TestCatalogTrimPathReplicaByReplicaIndex(t *testing.T) {
	service := newTestCatalogService(t, newCatalogTestFileSystem())

	_, err := service.CreatePathReplica(context.Background(), bearerRequestContext(), "/tempZone/home/alice/file.txt", PathReplicaCreateOptions{
		Resource: "archiveResc",
		Update:   true,
	})
	if err != nil {
		t.Fatalf("CreatePathReplica returned error: %v", err)
	}

	replicaNumber := int64(1)
	replicas, err := service.TrimPathReplica(context.Background(), bearerRequestContext(), "/tempZone/home/alice/file.txt", PathReplicaTrimOptions{
		ReplicaIndex: &replicaNumber,
		MinCopies:    1,
	})
	if err != nil {
		t.Fatalf("TrimPathReplica returned error: %v", err)
	}

	if len(replicas) != 1 {
		t.Fatalf("expected 1 replica after trim, got %+v", replicas)
	}
	if replicas[0].ResourceName != "demoResc" {
		t.Fatalf("expected demoResc to remain, got %+v", replicas)
	}
}

func TestCatalogCreatePathChildCollectionReturnsCreatedEntry(t *testing.T) {
	service := newTestCatalogService(t, newCatalogTestFileSystem())

	entry, err := service.CreatePathChild(context.Background(), bearerRequestContext(), "/tempZone/home/alice/project", PathCreateOptions{
		ChildName: "new-folder",
		Kind:      "collection",
	})
	if err != nil {
		t.Fatalf("CreatePathChild returned error: %v", err)
	}

	if entry.Path != "/tempZone/home/alice/project/new-folder" {
		t.Fatalf("expected created path, got %q", entry.Path)
	}
	if entry.Kind != "collection" {
		t.Fatalf("expected collection kind, got %q", entry.Kind)
	}
	if entry.ChildCount != 0 {
		t.Fatalf("expected empty collection child count, got %d", entry.ChildCount)
	}
}

func TestCatalogCreatePathChildCollectionSupportsMkdirs(t *testing.T) {
	service := newTestCatalogService(t, newCatalogTestFileSystem())

	entry, err := service.CreatePathChild(context.Background(), bearerRequestContext(), "/tempZone/home/alice/project", PathCreateOptions{
		ChildName: "nested/deeper",
		Kind:      "collection",
		Mkdirs:    true,
	})
	if err != nil {
		t.Fatalf("CreatePathChild with mkdirs returned error: %v", err)
	}

	if entry.Path != "/tempZone/home/alice/project/nested/deeper" {
		t.Fatalf("expected nested created path, got %q", entry.Path)
	}
}

func TestCatalogCreatePathChildDataObjectReturnsZeroByteEntry(t *testing.T) {
	service := newTestCatalogService(t, newCatalogTestFileSystem())

	entry, err := service.CreatePathChild(context.Background(), bearerRequestContext(), "/tempZone/home/alice/project", PathCreateOptions{
		ChildName: "empty.txt",
		Kind:      "data_object",
	})
	if err != nil {
		t.Fatalf("CreatePathChild returned error: %v", err)
	}

	if entry.Path != "/tempZone/home/alice/project/empty.txt" {
		t.Fatalf("expected created path, got %q", entry.Path)
	}
	if entry.Kind != "data_object" {
		t.Fatalf("expected data_object kind, got %q", entry.Kind)
	}
	if entry.Size != 0 || entry.DisplaySize != "0 B" {
		t.Fatalf("expected zero-byte file, got size=%d display=%q", entry.Size, entry.DisplaySize)
	}
}

func TestCatalogUploadPathContentsCreatesDataObjectWithChecksum(t *testing.T) {
	filesystem := newCatalogTestFileSystem()
	service := newTestCatalogService(t, filesystem)

	uploaded, err := service.UploadPathContents(context.Background(), bearerRequestContext(), "/tempZone/home/alice/project", PathContentsUploadOptions{
		FileName: "upload.txt",
		Content:  bytes.NewBufferString("upload payload"),
		Checksum: true,
	})
	if err != nil {
		t.Fatalf("UploadPathContents returned error: %v", err)
	}

	if uploaded.Path != "/tempZone/home/alice/project/upload.txt" {
		t.Fatalf("expected uploaded path, got %q", uploaded.Path)
	}
	if uploaded.Action != "created" {
		t.Fatalf("expected action created, got %q", uploaded.Action)
	}
	if uploaded.Size != int64(len("upload payload")) {
		t.Fatalf("expected uploaded size %d, got %d", len("upload payload"), uploaded.Size)
	}
	if uploaded.Checksum == nil || !uploaded.Checksum.Requested || !uploaded.Checksum.Verified {
		t.Fatalf("expected checksum verification info, got %+v", uploaded.Checksum)
	}
	if got := string(filesystem.contentByPath["/tempZone/home/alice/project/upload.txt"]); got != "upload payload" {
		t.Fatalf("expected stored payload, got %q", got)
	}
}

func TestCatalogUploadPathContentsReplacesExistingDataObjectWhenOverwriteEnabled(t *testing.T) {
	filesystem := newCatalogTestFileSystem()
	service := newTestCatalogService(t, filesystem)

	uploaded, err := service.UploadPathContents(context.Background(), bearerRequestContext(), "/tempZone/home/alice/project", PathContentsUploadOptions{
		FileName:  "child.txt",
		Content:   bytes.NewBufferString("replacement payload"),
		Overwrite: true,
	})
	if err != nil {
		t.Fatalf("UploadPathContents returned error: %v", err)
	}

	if uploaded.Action != "replaced" {
		t.Fatalf("expected action replaced, got %q", uploaded.Action)
	}
	if got := string(filesystem.contentByPath["/tempZone/home/alice/project/child.txt"]); got != "replacement payload" {
		t.Fatalf("expected replaced payload, got %q", got)
	}
}

func TestNormalizePathAccessErrorMapsDuplicateNameToConflict(t *testing.T) {
	err := normalizePathAccessError("create data object", "/tempZone/home/alice/project/file.txt", irodstypes.NewIRODSError(irodscommon.CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME))
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict, got %v", err)
	}
}

func TestCatalogDeletePathRemovesDataObject(t *testing.T) {
	filesystem := newCatalogTestFileSystem()
	service := newTestCatalogService(t, filesystem)

	if err := service.DeletePath(context.Background(), bearerRequestContext(), "/tempZone/home/alice/file.txt", false); err != nil {
		t.Fatalf("DeletePath returned error: %v", err)
	}

	if _, ok := filesystem.entriesByPath["/tempZone/home/alice/file.txt"]; ok {
		t.Fatal("expected file entry to be removed")
	}
}

func TestCatalogDeletePathRejectsNonEmptyCollectionWithoutForce(t *testing.T) {
	service := newTestCatalogService(t, newCatalogTestFileSystem())

	err := service.DeletePath(context.Background(), bearerRequestContext(), "/tempZone/home/alice/project", false)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict, got %v", err)
	}
}

func TestCatalogDeletePathRecursivelyRemovesCollectionWithForce(t *testing.T) {
	filesystem := newCatalogTestFileSystem()
	service := newTestCatalogService(t, filesystem)

	if err := service.DeletePath(context.Background(), bearerRequestContext(), "/tempZone/home/alice/project", true); err != nil {
		t.Fatalf("DeletePath returned error: %v", err)
	}

	if _, ok := filesystem.entriesByPath["/tempZone/home/alice/project"]; ok {
		t.Fatal("expected collection entry to be removed")
	}
	if _, ok := filesystem.entriesByPath["/tempZone/home/alice/project/child.txt"]; ok {
		t.Fatal("expected child entry to be removed")
	}
}

func TestCatalogRenamePathRenamesDataObject(t *testing.T) {
	filesystem := newCatalogTestFileSystem()
	service := newTestCatalogService(t, filesystem)

	entry, err := service.RenamePath(context.Background(), bearerRequestContext(), "/tempZone/home/alice/file.txt", "renamed.txt")
	if err != nil {
		t.Fatalf("RenamePath returned error: %v", err)
	}

	if entry.Path != "/tempZone/home/alice/renamed.txt" {
		t.Fatalf("expected renamed path, got %q", entry.Path)
	}
	if _, ok := filesystem.entriesByPath["/tempZone/home/alice/file.txt"]; ok {
		t.Fatal("expected original file path to be removed")
	}
}

func TestCatalogRenamePathRenamesCollection(t *testing.T) {
	filesystem := newCatalogTestFileSystem()
	service := newTestCatalogService(t, filesystem)

	entry, err := service.RenamePath(context.Background(), bearerRequestContext(), "/tempZone/home/alice/project", "renamed-project")
	if err != nil {
		t.Fatalf("RenamePath returned error: %v", err)
	}

	if entry.Path != "/tempZone/home/alice/renamed-project" {
		t.Fatalf("expected renamed collection path, got %q", entry.Path)
	}
	if _, ok := filesystem.entriesByPath["/tempZone/home/alice/project"]; ok {
		t.Fatal("expected original collection path to be removed")
	}
}

func TestCatalogRelocatePathMovesDataObjectToDestinationPath(t *testing.T) {
	filesystem := newCatalogTestFileSystem()
	service := newTestCatalogService(t, filesystem)

	entry, err := service.RelocatePath(context.Background(), bearerRequestContext(), "/tempZone/home/alice/file.txt", PathRelocateOptions{
		Operation:       PathRelocateOperationMove,
		DestinationPath: "/tempZone/home/alice/project/moved.txt",
	})
	if err != nil {
		t.Fatalf("RelocatePath returned error: %v", err)
	}

	if entry.Path != "/tempZone/home/alice/project/moved.txt" {
		t.Fatalf("expected moved path, got %q", entry.Path)
	}
	if _, ok := filesystem.entriesByPath["/tempZone/home/alice/file.txt"]; ok {
		t.Fatal("expected original file path to be removed")
	}
}

func TestCatalogRelocatePathCopiesDataObjectToDestinationPath(t *testing.T) {
	filesystem := newCatalogTestFileSystem()
	service := newTestCatalogService(t, filesystem)

	entry, err := service.RelocatePath(context.Background(), bearerRequestContext(), "/tempZone/home/alice/file.txt", PathRelocateOptions{
		Operation:       PathRelocateOperationCopy,
		DestinationPath: "/tempZone/home/alice/project/copied.txt",
	})
	if err != nil {
		t.Fatalf("RelocatePath returned error: %v", err)
	}

	if entry.Path != "/tempZone/home/alice/project/copied.txt" {
		t.Fatalf("expected copied path, got %q", entry.Path)
	}
	if _, ok := filesystem.entriesByPath["/tempZone/home/alice/file.txt"]; !ok {
		t.Fatal("expected source file to remain after copy")
	}
}

func TestCatalogRelocatePathCopiesCollectionRecursively(t *testing.T) {
	filesystem := newCatalogTestFileSystem()
	service := newTestCatalogService(t, filesystem)

	entry, err := service.RelocatePath(context.Background(), bearerRequestContext(), "/tempZone/home/alice/project", PathRelocateOptions{
		Operation:       PathRelocateOperationCopy,
		DestinationPath: "/tempZone/home/alice/project-copy",
	})
	if err != nil {
		t.Fatalf("RelocatePath returned error: %v", err)
	}

	if entry.Path != "/tempZone/home/alice/project-copy" {
		t.Fatalf("expected copied collection path, got %q", entry.Path)
	}
	if _, ok := filesystem.entriesByPath["/tempZone/home/alice/project/child.txt"]; !ok {
		t.Fatal("expected source child to remain")
	}
	if _, ok := filesystem.entriesByPath["/tempZone/home/alice/project-copy/child.txt"]; !ok {
		t.Fatal("expected copied child entry to exist")
	}
}

func TestCatalogGetPathMetadataMapsAVUs(t *testing.T) {
	service := newTestCatalogService(t, newCatalogTestFileSystem())

	metadata, err := service.GetPathMetadata(context.Background(), bearerRequestContext(), "/tempZone/home/alice/file.txt")
	if err != nil {
		t.Fatalf("GetPathMetadata returned error: %v", err)
	}

	if len(metadata) != 1 {
		t.Fatalf("expected 1 AVU row, got %d", len(metadata))
	}
	if metadata[0].ID != "501" {
		t.Fatalf("expected AVU id 501, got %q", metadata[0].ID)
	}
	if metadata[0].Attrib != "source" {
		t.Fatalf("expected attrib source, got %q", metadata[0].Attrib)
	}
	if metadata[0].Value != "unit-test" {
		t.Fatalf("expected value unit-test, got %q", metadata[0].Value)
	}
	if metadata[0].Unit != "system" {
		t.Fatalf("expected unit system, got %q", metadata[0].Unit)
	}
	if metadata[0].CreatedAt == nil || metadata[0].UpdatedAt == nil {
		t.Fatal("expected AVU timestamps to be populated")
	}
}

func TestCatalogGetPathACLMapsUsersAndGroups(t *testing.T) {
	service := newTestCatalogService(t, newCatalogTestFileSystem())

	acl, err := service.GetPathACL(context.Background(), bearerRequestContext(), "/tempZone/home/alice/file.txt")
	if err != nil {
		t.Fatalf("GetPathACL returned error: %v", err)
	}

	if acl.IRODSPath != "/tempZone/home/alice/file.txt" {
		t.Fatalf("expected ACL path to be populated, got %q", acl.IRODSPath)
	}
	if acl.Kind != "data_object" {
		t.Fatalf("expected data_object ACL kind, got %q", acl.Kind)
	}
	if len(acl.Users) != 1 {
		t.Fatalf("expected 1 user ACL, got %+v", acl.Users)
	}
	if acl.Users[0].ID != "user:tempZone:alice" || acl.Users[0].Name != "alice" || acl.Users[0].Type != "user" || acl.Users[0].AccessLevel != "own" {
		t.Fatalf("unexpected user ACL entry: %+v", acl.Users[0])
	}
	if len(acl.Groups) != 1 {
		t.Fatalf("expected 1 group ACL, got %+v", acl.Groups)
	}
	if acl.Groups[0].ID != "group:tempZone:research-team" || acl.Groups[0].Name != "research-team" || acl.Groups[0].Type != "group" || acl.Groups[0].AccessLevel != "read_object" {
		t.Fatalf("unexpected group ACL entry: %+v", acl.Groups[0])
	}
}

func TestCatalogAddPathMetadataReturnsCreatedAVU(t *testing.T) {
	service := newTestCatalogService(t, newCatalogTestFileSystem())

	created, err := service.AddPathMetadata(context.Background(), bearerRequestContext(), "/tempZone/home/alice/file.txt", "new-attr", "new-value", "new-unit")
	if err != nil {
		t.Fatalf("AddPathMetadata returned error: %v", err)
	}

	if created.Attrib != "new-attr" || created.Value != "new-value" || created.Unit != "new-unit" {
		t.Fatalf("unexpected created avu %+v", created)
	}
	if created.ID == "" {
		t.Fatal("expected created AVU id to be populated")
	}
}

func TestCatalogDeletePathMetadataRemovesAVU(t *testing.T) {
	service := newTestCatalogService(t, newCatalogTestFileSystem())

	if err := service.DeletePathMetadata(context.Background(), bearerRequestContext(), "/tempZone/home/alice/file.txt", "501"); err != nil {
		t.Fatalf("DeletePathMetadata returned error: %v", err)
	}

	metadata, err := service.GetPathMetadata(context.Background(), bearerRequestContext(), "/tempZone/home/alice/file.txt")
	if err != nil {
		t.Fatalf("GetPathMetadata returned error: %v", err)
	}
	if len(metadata) != 0 {
		t.Fatalf("expected AVU to be removed, got %d rows", len(metadata))
	}
}

func TestCatalogUpdatePathMetadataReplacesAVU(t *testing.T) {
	service := newTestCatalogService(t, newCatalogTestFileSystem())

	updated, err := service.UpdatePathMetadata(context.Background(), bearerRequestContext(), "/tempZone/home/alice/file.txt", "501", "source", "updated", "system")
	if err != nil {
		t.Fatalf("UpdatePathMetadata returned error: %v", err)
	}
	if updated.Value != "updated" {
		t.Fatalf("expected updated AVU value, got %+v", updated)
	}

	metadata, err := service.GetPathMetadata(context.Background(), bearerRequestContext(), "/tempZone/home/alice/file.txt")
	if err != nil {
		t.Fatalf("GetPathMetadata returned error: %v", err)
	}
	if len(metadata) != 1 || metadata[0].Value != "updated" {
		t.Fatalf("expected updated metadata row, got %+v", metadata)
	}
}

func TestCatalogGetPathChecksumReturnsTypedChecksum(t *testing.T) {
	service := newTestCatalogService(t, newCatalogTestFileSystem())

	checksum, err := service.GetPathChecksum(context.Background(), bearerRequestContext(), "/tempZone/home/alice/file.txt")
	if err != nil {
		t.Fatalf("GetPathChecksum returned error: %v", err)
	}

	if checksum.Checksum != "sha2:YWJjMTIz" {
		t.Fatalf("expected checksum sha2:YWJjMTIz, got %q", checksum.Checksum)
	}
	if checksum.Type != "sha2" {
		t.Fatalf("expected checksum type sha2, got %q", checksum.Type)
	}
}

func TestCatalogComputePathChecksumUpdatesDisplayChecksum(t *testing.T) {
	filesystem := newCatalogTestFileSystem()
	file := filesystem.entriesByPath["/tempZone/home/alice/file.txt"]
	file.CheckSum = nil
	file.CheckSumAlgorithm = irodstypes.ChecksumAlgorithmUnknown

	service := newTestCatalogService(t, filesystem)

	computed, err := service.ComputePathChecksum(context.Background(), bearerRequestContext(), "/tempZone/home/alice/file.txt")
	if err != nil {
		t.Fatalf("ComputePathChecksum returned error: %v", err)
	}
	if computed.Checksum != "sha2:ZGVmNDU2" {
		t.Fatalf("expected computed checksum sha2:ZGVmNDU2, got %q", computed.Checksum)
	}
	if computed.Type != "sha2" {
		t.Fatalf("expected computed checksum type sha2, got %q", computed.Type)
	}

	entry, err := service.GetPath(context.Background(), bearerRequestContext(), "/tempZone/home/alice/file.txt", PathLookupOptions{})
	if err != nil {
		t.Fatalf("GetPath returned error after compute: %v", err)
	}
	if entry.Checksum == nil || entry.Checksum.Checksum != computed.Checksum {
		t.Fatalf("expected path checksum %q after compute, got %+v", computed.Checksum, entry.Checksum)
	}
}

func TestHumanReadableSize(t *testing.T) {
	for _, tc := range []struct {
		size     int64
		expected string
	}{
		{0, "0 B"},
		{999, "999 B"},
		{1024, "1 KB"},
		{1536, "1.5 KB"},
		{1048576, "1 MB"},
		{1610612736, "1.5 GB"},
	} {
		if got := humanReadableSize(tc.size); got != tc.expected {
			t.Fatalf("size %d: expected %q, got %q", tc.size, tc.expected, got)
		}
	}
}

func TestCatalogGetPathNormalizesNotFound(t *testing.T) {
	service := newTestCatalogService(t, newCatalogTestFileSystem())

	_, err := service.GetPath(context.Background(), bearerRequestContext(), "/tempZone/home/alice/missing", PathLookupOptions{})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestCatalogGetPathNormalizesPermissionDenied(t *testing.T) {
	service := newTestCatalogService(t, newCatalogTestFileSystem())

	_, err := service.GetPath(context.Background(), bearerRequestContext(), "/tempZone/home/alice/forbidden", PathLookupOptions{})
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
	if content.FileName != "file.txt" {
		t.Fatalf("expected file name file.txt, got %q", content.FileName)
	}
	if content.Checksum == nil || content.Checksum.Checksum == "" {
		t.Fatalf("expected checksum in object content, got %+v", content.Checksum)
	}
	if content.UpdatedAt == nil {
		t.Fatal("expected updated timestamp in object content")
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

func TestCatalogCreateAnonymousTicketReturnsCreatedTicket(t *testing.T) {
	filesystem := newCatalogTestFileSystem()
	service := newTestCatalogService(t, filesystem).(TicketService)

	ticketNow = func() time.Time { return time.Date(2026, 4, 28, 12, 0, 0, 0, time.UTC) }
	defer func() { ticketNow = time.Now }()

	ticket, err := service.CreateAnonymousTicket(context.Background(), bearerRequestContext(), "/tempZone/home/alice/file.txt", TicketCreateOptions{
		MaximumUses:     5,
		LifetimeMinutes: 30,
	})
	if err != nil {
		t.Fatalf("CreateAnonymousTicket returned error: %v", err)
	}

	if ticket.Name == "" || !strings.HasPrefix(ticket.Name, "ticket_") {
		t.Fatalf("expected generated ticket name, got %+v", ticket)
	}
	if ticket.BearerToken == "" || !strings.HasPrefix(ticket.BearerToken, "irods-ticket:ticket_") {
		t.Fatalf("expected bearer token in ticket response, got %+v", ticket)
	}
	if ticket.Path != "/tempZone/home/alice/file.txt" || ticket.UsesLimit != 5 {
		t.Fatalf("unexpected ticket mapping %+v", ticket)
	}
	if ticket.ExpirationTime == nil {
		t.Fatalf("expected expiration time on created ticket, got %+v", ticket)
	}
}

func TestCatalogListTicketsFiltersToRequestOwner(t *testing.T) {
	filesystem := newCatalogTestFileSystem()
	filesystem.ticketsByName["ticket-other"] = &irodstypes.IRODSTicket{
		Name:  "ticket-other",
		Owner: "bob",
	}

	service := newTestCatalogService(t, filesystem).(TicketService)

	tickets, err := service.ListTickets(context.Background(), bearerRequestContext())
	if err != nil {
		t.Fatalf("ListTickets returned error: %v", err)
	}
	if len(tickets) != 1 || tickets[0].Name != "ticket-existing" {
		t.Fatalf("expected only alice-owned tickets, got %+v", tickets)
	}
}

func TestCatalogAccountForRequestAppliesSSLConnectionConfig(t *testing.T) {
	cfg := config.RestConfig{
		IrodsZone:              "tempZone",
		IrodsHost:              "irods.local",
		IrodsPort:              1247,
		IrodsAuthScheme:        "native",
		IrodsNegotiationPolicy: "CS_NEG_REQUIRE",
		IrodsAdminUser:         "rods",
		IrodsAdminPassword:     "rods",
		IrodsDefaultResource:   "demoResc",
		IrodsSSLConfig: config.IrodsSSLConfig{
			CACertificateFile:   "/etc/irods/ca.pem",
			EncryptionKeySize:   32,
			EncryptionAlgorithm: "AES-256-CBC",
			EncryptionSaltSize:  8,
			VerifyServer:        "hostname",
			ServerName:          "irods.ssl.local",
		},
	}
	service := &catalogService{cfg: cfg}

	tests := []struct {
		name       string
		ctx        *RequestContext
		wantClient string
		wantProxy  string
		wantTicket string
	}{
		{
			name:       "basic",
			ctx:        &RequestContext{AuthScheme: "basic", Username: "alice", BasicPassword: "secret"},
			wantClient: "alice",
			wantProxy:  "alice",
		},
		{
			name:       "ticket",
			ctx:        &RequestContext{AuthScheme: "bearer-ticket", Ticket: "ticket-token"},
			wantClient: "rods",
			wantProxy:  "rods",
			wantTicket: "ticket-token",
		},
		{
			name:       "bearer",
			ctx:        &RequestContext{AuthScheme: "bearer", Username: "alice"},
			wantClient: "alice",
			wantProxy:  "rods",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			account, err := service.accountForRequest(tt.ctx)
			if err != nil {
				t.Fatalf("accountForRequest returned error: %v", err)
			}

			if !account.ClientServerNegotiation {
				t.Fatal("expected client-server negotiation for SSL policy")
			}
			if account.CSNegotiationPolicy != irodstypes.CSNegotiationPolicyRequestSSL {
				t.Fatalf("expected SSL negotiation policy, got %q", account.CSNegotiationPolicy)
			}
			if account.SSLConfiguration == nil {
				t.Fatal("expected SSL configuration on account")
			}
			if account.SSLConfiguration.CACertificateFile != "/etc/irods/ca.pem" {
				t.Fatalf("expected CA file on account, got %q", account.SSLConfiguration.CACertificateFile)
			}
			if account.SSLConfiguration.ServerName != "irods.ssl.local" {
				t.Fatalf("expected SSL server name on account, got %q", account.SSLConfiguration.ServerName)
			}
			if account.ClientUser != tt.wantClient || account.ProxyUser != tt.wantProxy {
				t.Fatalf("unexpected account user mapping: client=%q proxy=%q", account.ClientUser, account.ProxyUser)
			}
			if account.Ticket != tt.wantTicket {
				t.Fatalf("expected ticket %q, got %q", tt.wantTicket, account.Ticket)
			}
		})
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
	aclByPath      map[string][]*irodstypes.IRODSAccess
	inheritByPath  map[string]bool
	contentByPath  map[string][]byte
	ticketsByName  map[string]*irodstypes.IRODSTicket
	resources      []*irodstypes.IRODSResource
	usersByKey     map[string]*irodstypes.IRODSUser
	groupMembers   map[string][]string
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
		Owner:             "alice",
		Path:              "/tempZone/home/alice/file.txt",
		Size:              21,
		DataType:          "generic",
		CheckSumAlgorithm: irodstypes.ChecksumAlgorithmSHA256,
		CheckSum:          []byte("abc123"),
		IRODSReplicas: []irodstypes.IRODSReplica{{
			Number:            0,
			Owner:             "alice",
			Status:            "1",
			ResourceName:      "demoResc",
			ResourceHierarchy: "demoResc",
			Path:              "/var/lib/irods/Vault/home/alice/file.txt",
			Checksum: &irodstypes.IRODSChecksum{
				Algorithm:           irodstypes.ChecksumAlgorithmSHA256,
				IRODSChecksumString: "sha2:YWJjMTIz",
			},
			ModifyTime: now,
		}},
		CreateTime: now,
		ModifyTime: now,
	}
	child := &irodsfs.Entry{
		ID:       102,
		Type:     irodsfs.FileEntry,
		Name:     "child.txt",
		Owner:    "alice",
		Path:     "/tempZone/home/alice/project/child.txt",
		Size:     10,
		DataType: "generic",
		IRODSReplicas: []irodstypes.IRODSReplica{{
			Number:            2,
			Owner:             "alice",
			Status:            "2",
			ResourceName:      "repl1",
			ResourceHierarchy: "repl1;child1",
			Path:              "/var/lib/irods/child1vault/public/foo",
			ModifyTime:        now,
		}},
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
				AVUID:      500,
				Name:       "project",
				Value:      "demo",
				Units:      "folder",
				CreateTime: now,
				ModifyTime: now,
			}},
			file.Path: {{
				AVUID:      501,
				Name:       "source",
				Value:      "unit-test",
				Units:      "system",
				CreateTime: now,
				ModifyTime: now,
			}},
		},
		aclByPath: map[string][]*irodstypes.IRODSAccess{
			file.Path: {
				{
					Path:        file.Path,
					UserName:    "alice",
					UserZone:    "tempZone",
					UserType:    irodstypes.IRODSUserRodsUser,
					AccessLevel: irodstypes.IRODSAccessLevelOwner,
				},
				{
					Path:        file.Path,
					UserName:    "research-team",
					UserZone:    "tempZone",
					UserType:    irodstypes.IRODSUserRodsGroup,
					AccessLevel: irodstypes.IRODSAccessLevelReadObject,
				},
			},
		},
		inheritByPath: map[string]bool{
			project.Path: false,
			nested.Path:  true,
		},
		contentByPath: map[string][]byte{
			file.Path: []byte("hello content payload"),
		},
		ticketsByName: map[string]*irodstypes.IRODSTicket{
			"ticket-existing": {
				ID:             900,
				Name:           "ticket-existing",
				Type:           irodstypes.TicketTypeRead,
				Owner:          "alice",
				OwnerZone:      "tempZone",
				ObjectType:     "data",
				Path:           file.Path,
				UsesLimit:      5,
				UsesCount:      1,
				WriteFileLimit: 10,
				ExpirationTime: now.Add(30 * time.Minute),
			},
		},
		usersByKey: map[string]*irodstypes.IRODSUser{
			catalogUserKey("alice", "tempZone"): {
				ID:   300,
				Name: "alice",
				Zone: "tempZone",
				Type: irodstypes.IRODSUserRodsUser,
			},
			catalogUserKey("alicia", "tempZone"): {
				ID:   301,
				Name: "alicia",
				Zone: "tempZone",
				Type: irodstypes.IRODSUserRodsUser,
			},
			catalogUserKey("bob", "tempZone"): {
				ID:   302,
				Name: "bob",
				Zone: "tempZone",
				Type: irodstypes.IRODSUserRodsUser,
			},
			catalogUserKey("rods", "tempZone"): {
				ID:   303,
				Name: "rods",
				Zone: "tempZone",
				Type: irodstypes.IRODSUserRodsAdmin,
			},
			catalogUserKey("groupadmin", "tempZone"): {
				ID:   304,
				Name: "groupadmin",
				Zone: "tempZone",
				Type: irodstypes.IRODSUserGroupAdmin,
			},
			catalogUserKey("research-team", "tempZone"): {
				ID:   305,
				Name: "research-team",
				Zone: "tempZone",
				Type: irodstypes.IRODSUserRodsGroup,
			},
		},
		groupMembers: map[string][]string{
			catalogUserKey("research-team", "tempZone"): {"alice"},
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

func (f *catalogTestFileSystem) MakeDir(irodsPath string, recurse bool) error {
	if irodsPath == "/tempZone/home/alice/forbidden" {
		return irodstypes.NewIRODSError(irodscommon.CAT_NO_ACCESS_PERMISSION)
	}

	cleaned := path.Clean(irodsPath)
	parentPath := path.Dir(cleaned)
	if recurse {
		current := parentPath
		for current != "." && current != "/" && current != cleaned {
			if _, ok := f.entriesByPath[current]; ok {
				break
			}
			segments := []string{current}
			for {
				next := path.Dir(current)
				if next == "." || next == "/" || next == current {
					break
				}
				if _, ok := f.entriesByPath[next]; ok {
					break
				}
				segments = append(segments, next)
				current = next
			}
			for i := len(segments) - 1; i >= 0; i-- {
				if err := f.MakeDir(segments[i], false); err != nil {
					return err
				}
			}
			break
		}
	}

	if parentPath != "." && parentPath != "/" {
		parentEntry, ok := f.entriesByPath[parentPath]
		if !ok || !parentEntry.IsDir() {
			return irodstypes.NewFileNotFoundError(parentPath)
		}
	}

	now := time.Unix(1_700_000_002, 0)
	entry := &irodsfs.Entry{
		ID:         int64(len(f.entriesByPath) + 200),
		Type:       irodsfs.DirectoryEntry,
		Name:       path.Base(cleaned),
		Path:       cleaned,
		CreateTime: now,
		ModifyTime: now,
	}
	f.entriesByPath[cleaned] = entry
	if _, ok := f.childrenByPath[cleaned]; !ok {
		f.childrenByPath[cleaned] = []*irodsfs.Entry{}
	}
	if parentPath != "." && parentPath != "/" {
		f.childrenByPath[parentPath] = append(f.childrenByPath[parentPath], entry)
	}
	return nil
}

func (f *catalogTestFileSystem) CreateFile(irodsPath string, _ string, _ string) (CatalogFileHandle, error) {
	if irodsPath == "/tempZone/home/alice/forbidden" {
		return nil, irodstypes.NewIRODSError(irodscommon.CAT_NO_ACCESS_PERMISSION)
	}

	cleaned := path.Clean(irodsPath)
	parentPath := path.Dir(cleaned)
	parentEntry, ok := f.entriesByPath[parentPath]
	if !ok || !parentEntry.IsDir() {
		return nil, irodstypes.NewFileNotFoundError(parentPath)
	}

	now := time.Unix(1_700_000_002, 0)
	entry := &irodsfs.Entry{
		ID:         int64(len(f.entriesByPath) + 200),
		Type:       irodsfs.FileEntry,
		Name:       path.Base(cleaned),
		Path:       cleaned,
		Owner:      "alice",
		Size:       0,
		DataType:   "generic",
		CreateTime: now,
		ModifyTime: now,
	}
	f.entriesByPath[cleaned] = entry
	f.childrenByPath[parentPath] = append(f.childrenByPath[parentPath], entry)
	f.contentByPath[cleaned] = nil

	return &catalogTestFileHandle{
		reader: bytes.NewReader(nil),
		writer: bytes.NewBuffer(nil),
		onClose: func(data []byte) {
			f.contentByPath[cleaned] = append([]byte(nil), data...)
			entry.Size = int64(len(data))
		},
	}, nil
}

func (f *catalogTestFileSystem) RemoveDir(irodsPath string, recurse bool, _ bool) error {
	if irodsPath == "/tempZone/home/alice/forbidden" {
		return irodstypes.NewIRODSError(irodscommon.CAT_NO_ACCESS_PERMISSION)
	}

	entry, ok := f.entriesByPath[irodsPath]
	if !ok || !entry.IsDir() {
		return irodstypes.NewFileNotFoundError(irodsPath)
	}
	if !recurse {
		if children := f.childrenByPath[irodsPath]; len(children) > 0 {
			return errors.New("collection not empty")
		}
	}
	f.removeDirRecursive(path.Clean(irodsPath))
	return nil
}

func (f *catalogTestFileSystem) RemoveFile(irodsPath string, _ bool) error {
	if irodsPath == "/tempZone/home/alice/forbidden" {
		return irodstypes.NewIRODSError(irodscommon.CAT_NO_ACCESS_PERMISSION)
	}

	entry, ok := f.entriesByPath[irodsPath]
	if !ok || entry.IsDir() {
		return irodstypes.NewFileNotFoundError(irodsPath)
	}

	delete(f.entriesByPath, path.Clean(irodsPath))
	delete(f.contentByPath, path.Clean(irodsPath))
	delete(f.metadataByPath, path.Clean(irodsPath))
	parentPath := path.Dir(path.Clean(irodsPath))
	f.childrenByPath[parentPath] = filterCatalogChildEntry(f.childrenByPath[parentPath], path.Clean(irodsPath))
	return nil
}

func (f *catalogTestFileSystem) RenameDir(srcPath string, destPath string) error {
	entry, ok := f.entriesByPath[srcPath]
	if !ok || !entry.IsDir() {
		return irodstypes.NewFileNotFoundError(srcPath)
	}
	f.renameDirRecursive(path.Clean(srcPath), path.Clean(destPath))
	return nil
}

func (f *catalogTestFileSystem) RenameFile(srcPath string, destPath string) error {
	entry, ok := f.entriesByPath[srcPath]
	if !ok || entry.IsDir() {
		return irodstypes.NewFileNotFoundError(srcPath)
	}

	cleanSrc := path.Clean(srcPath)
	cleanDest := path.Clean(destPath)
	parentSrc := path.Dir(cleanSrc)
	parentDest := path.Dir(cleanDest)

	entry.Path = cleanDest
	entry.Name = path.Base(cleanDest)
	f.entriesByPath[cleanDest] = entry
	delete(f.entriesByPath, cleanSrc)

	if data, ok := f.contentByPath[cleanSrc]; ok {
		f.contentByPath[cleanDest] = data
		delete(f.contentByPath, cleanSrc)
	}
	if metas, ok := f.metadataByPath[cleanSrc]; ok {
		f.metadataByPath[cleanDest] = metas
		delete(f.metadataByPath, cleanSrc)
	}

	f.childrenByPath[parentSrc] = filterCatalogChildEntry(f.childrenByPath[parentSrc], cleanSrc)
	f.childrenByPath[parentDest] = append(f.childrenByPath[parentDest], entry)
	return nil
}

func (f *catalogTestFileSystem) CopyFile(srcPath string, destPath string, force bool) error {
	entry, ok := f.entriesByPath[srcPath]
	if !ok || entry.IsDir() {
		return irodstypes.NewFileNotFoundError(srcPath)
	}

	cleanDest := path.Clean(destPath)
	if _, exists := f.entriesByPath[cleanDest]; exists && !force {
		return irodstypes.NewIRODSError(irodscommon.CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME)
	}

	parentDest := path.Dir(cleanDest)
	parentEntry, ok := f.entriesByPath[parentDest]
	if !ok || !parentEntry.IsDir() {
		return irodstypes.NewFileNotFoundError(parentDest)
	}

	cloned := *entry
	cloned.Path = cleanDest
	cloned.Name = path.Base(cleanDest)
	f.entriesByPath[cleanDest] = &cloned
	f.childrenByPath[parentDest] = append(f.childrenByPath[parentDest], &cloned)

	if data, ok := f.contentByPath[path.Clean(srcPath)]; ok {
		f.contentByPath[cleanDest] = append([]byte(nil), data...)
	}
	if metas, ok := f.metadataByPath[path.Clean(srcPath)]; ok {
		clonedMetas := make([]*irodstypes.IRODSMeta, 0, len(metas))
		for _, meta := range metas {
			if meta == nil {
				continue
			}
			cloneMeta := *meta
			clonedMetas = append(clonedMetas, &cloneMeta)
		}
		f.metadataByPath[cleanDest] = clonedMetas
	}

	return nil
}

func (f *catalogTestFileSystem) ReplicateFile(irodsPath string, resource string, _ bool) error {
	resource = strings.TrimSpace(resource)
	if resource == "" {
		return errors.New("resource is required")
	}

	entry, ok := f.entriesByPath[irodsPath]
	if !ok || entry.IsDir() {
		return irodstypes.NewFileNotFoundError(irodsPath)
	}

	for _, replica := range entry.IRODSReplicas {
		if strings.TrimSpace(replica.ResourceName) == resource {
			return nil
		}
	}

	nextReplicaNumber := int64(0)
	for _, replica := range entry.IRODSReplicas {
		if replica.Number >= nextReplicaNumber {
			nextReplicaNumber = replica.Number + 1
		}
	}

	now := time.Unix(1_700_000_003, 0)
	entry.IRODSReplicas = append(entry.IRODSReplicas, irodstypes.IRODSReplica{
		Number:            nextReplicaNumber,
		Owner:             entry.Owner,
		Status:            "1",
		ResourceName:      resource,
		ResourceHierarchy: resource,
		Path:              "/var/lib/irods/" + resource + "/Vault" + entry.Path,
		ModifyTime:        now,
	})
	entry.ModifyTime = now
	return nil
}

func (f *catalogTestFileSystem) TrimDataObject(irodsPath string, resource string, minCopies int, _ int) error {
	resource = strings.TrimSpace(resource)
	if resource == "" {
		return errors.New("resource is required")
	}

	entry, ok := f.entriesByPath[irodsPath]
	if !ok || entry.IsDir() {
		return irodstypes.NewFileNotFoundError(irodsPath)
	}

	if minCopies < 0 {
		minCopies = 0
	}

	replicas := entry.IRODSReplicas
	filtered := make([]irodstypes.IRODSReplica, 0, len(replicas))
	removed := false

	for _, replica := range replicas {
		if !removed && strings.TrimSpace(replica.ResourceName) == resource && len(replicas)-1 >= minCopies {
			removed = true
			continue
		}
		filtered = append(filtered, replica)
	}

	if !removed {
		return irodstypes.NewFileNotFoundError(irodsPath)
	}

	entry.IRODSReplicas = filtered
	entry.ModifyTime = time.Unix(1_700_000_004, 0)
	return nil
}

func (f *catalogTestFileSystem) ListMetadata(irodsPath string) ([]*irodstypes.IRODSMeta, error) {
	if irodsPath == "/tempZone/home/alice/forbidden" {
		return nil, irodstypes.NewIRODSError(irodscommon.CAT_NO_ACCESS_PERMISSION)
	}

	return f.metadataByPath[irodsPath], nil
}

func (f *catalogTestFileSystem) AddMetadata(irodsPath string, attName string, attValue string, attUnits string) error {
	if irodsPath == "/tempZone/home/alice/forbidden" {
		return irodstypes.NewIRODSError(irodscommon.CAT_NO_ACCESS_PERMISSION)
	}
	if _, ok := f.entriesByPath[irodsPath]; !ok {
		return irodstypes.NewFileNotFoundError(irodsPath)
	}

	nextID := int64(1)
	for _, meta := range f.metadataByPath[irodsPath] {
		if meta != nil && meta.AVUID >= nextID {
			nextID = meta.AVUID + 1
		}
	}

	now := time.Unix(1_700_000_001, 0)
	f.metadataByPath[irodsPath] = append(f.metadataByPath[irodsPath], &irodstypes.IRODSMeta{
		AVUID:      nextID,
		Name:       attName,
		Value:      attValue,
		Units:      attUnits,
		CreateTime: now,
		ModifyTime: now,
	})
	return nil
}

func (f *catalogTestFileSystem) DeleteMetadata(irodsPath string, avuID int64) error {
	if irodsPath == "/tempZone/home/alice/forbidden" {
		return irodstypes.NewIRODSError(irodscommon.CAT_NO_ACCESS_PERMISSION)
	}
	metas := f.metadataByPath[irodsPath]
	filtered := metas[:0]
	found := false
	for _, meta := range metas {
		if meta != nil && meta.AVUID == avuID {
			found = true
			continue
		}
		filtered = append(filtered, meta)
	}
	if !found {
		return irodstypes.NewFileNotFoundError(irodsPath)
	}
	f.metadataByPath[irodsPath] = filtered
	return nil
}

func (f *catalogTestFileSystem) ListACLs(irodsPath string) ([]*irodstypes.IRODSAccess, error) {
	if irodsPath == "/tempZone/home/alice/forbidden" {
		return nil, irodstypes.NewIRODSError(irodscommon.CAT_NO_ACCESS_PERMISSION)
	}
	if _, ok := f.entriesByPath[irodsPath]; !ok {
		return nil, irodstypes.NewFileNotFoundError(irodsPath)
	}
	return f.aclByPath[irodsPath], nil
}

func (f *catalogTestFileSystem) ChangeACLs(irodsPath string, access irodstypes.IRODSAccessLevelType, userName string, zoneName string, _ bool, _ bool) error {
	if irodsPath == "/tempZone/home/alice/forbidden" {
		return irodstypes.NewIRODSError(irodscommon.CAT_NO_ACCESS_PERMISSION)
	}
	if _, ok := f.entriesByPath[irodsPath]; !ok {
		return irodstypes.NewFileNotFoundError(irodsPath)
	}

	current := f.aclByPath[irodsPath]
	filtered := current[:0]
	for _, acl := range current {
		if acl != nil && acl.UserName == userName && acl.UserZone == zoneName {
			continue
		}
		filtered = append(filtered, acl)
	}
	f.aclByPath[irodsPath] = filtered

	if access == irodstypes.IRODSAccessLevelNull {
		return nil
	}

	user, ok := f.usersByKey[catalogUserKey(userName, zoneName)]
	userType := irodstypes.IRODSUserRodsUser
	if ok && user != nil {
		userType = user.Type
	}

	f.aclByPath[irodsPath] = append(f.aclByPath[irodsPath], &irodstypes.IRODSAccess{
		Path:        irodsPath,
		UserName:    userName,
		UserZone:    zoneName,
		UserType:    userType,
		AccessLevel: access,
	})
	return nil
}

func (f *catalogTestFileSystem) ChangeDirACLInheritance(irodsPath string, inherit bool, _ bool, _ bool) error {
	if irodsPath == "/tempZone/home/alice/forbidden" {
		return irodstypes.NewIRODSError(irodscommon.CAT_NO_ACCESS_PERMISSION)
	}
	entry, ok := f.entriesByPath[irodsPath]
	if !ok || !entry.IsDir() {
		return irodstypes.NewFileNotFoundError(irodsPath)
	}
	f.inheritByPath[irodsPath] = inherit
	return nil
}

func (f *catalogTestFileSystem) GetDirACLInheritance(irodsPath string) (*irodstypes.IRODSAccessInheritance, error) {
	if irodsPath == "/tempZone/home/alice/forbidden" {
		return nil, irodstypes.NewIRODSError(irodscommon.CAT_NO_ACCESS_PERMISSION)
	}
	entry, ok := f.entriesByPath[irodsPath]
	if !ok || !entry.IsDir() {
		return nil, irodstypes.NewFileNotFoundError(irodsPath)
	}

	return &irodstypes.IRODSAccessInheritance{
		Path:        irodsPath,
		Inheritance: f.inheritByPath[irodsPath],
	}, nil
}

func (f *catalogTestFileSystem) ComputeChecksum(irodsPath string, _ string) (*irodstypes.IRODSChecksum, error) {
	if irodsPath == "/tempZone/home/alice/forbidden" {
		return nil, irodstypes.NewIRODSError(irodscommon.CAT_NO_ACCESS_PERMISSION)
	}

	entry, ok := f.entriesByPath[irodsPath]
	if !ok {
		return nil, irodstypes.NewFileNotFoundError(irodsPath)
	}
	if entry.IsDir() {
		return nil, irodstypes.NewFileNotFoundError(irodsPath)
	}

	entry.CheckSumAlgorithm = irodstypes.ChecksumAlgorithmSHA256
	entry.CheckSum = []byte("def456")
	if len(entry.IRODSReplicas) > 0 {
		entry.IRODSReplicas[0].Checksum = &irodstypes.IRODSChecksum{
			Algorithm:           irodstypes.ChecksumAlgorithmSHA256,
			IRODSChecksumString: "sha2:ZGVmNDU2",
		}
	}

	return &irodstypes.IRODSChecksum{
		Algorithm:           irodstypes.ChecksumAlgorithmSHA256,
		IRODSChecksumString: "sha2:ZGVmNDU2",
	}, nil
}

func (f *catalogTestFileSystem) GetServerVersion() (*irodstypes.IRODSVersion, error) {
	return &irodstypes.IRODSVersion{
		ReleaseVersion: "rods4.3.2",
		APIVersion:     "d",
		ReconnectPort:  1247,
		ReconnectAddr:  "irods.example.org",
		Cookie:         734,
	}, nil
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

func (f *catalogTestFileSystem) GetUser(username string, zoneName string, _ irodstypes.IRODSUserType) (*irodstypes.IRODSUser, error) {
	user, ok := f.usersByKey[catalogUserKey(username, zoneName)]
	if !ok {
		return nil, irodstypes.NewUserNotFoundError(username)
	}
	return user, nil
}

func (f *catalogTestFileSystem) ListUsers(zoneName string, userType irodstypes.IRODSUserType) ([]*irodstypes.IRODSUser, error) {
	users := make([]*irodstypes.IRODSUser, 0, len(f.usersByKey))
	for _, user := range f.usersByKey {
		if user == nil || user.Zone != zoneName || user.Type != userType {
			continue
		}
		users = append(users, user)
	}
	return users, nil
}

func (f *catalogTestFileSystem) ListGroupMembers(zoneName string, groupName string) ([]*irodstypes.IRODSUser, error) {
	key := catalogUserKey(groupName, zoneName)
	usernames := f.groupMembers[key]
	members := make([]*irodstypes.IRODSUser, 0, len(usernames))
	for _, username := range usernames {
		user, ok := f.usersByKey[catalogUserKey(username, zoneName)]
		if !ok {
			continue
		}
		members = append(members, user)
	}
	return members, nil
}

func (f *catalogTestFileSystem) CreateUser(username string, zoneName string, userType irodstypes.IRODSUserType) (*irodstypes.IRODSUser, error) {
	key := catalogUserKey(username, zoneName)
	if existing, ok := f.usersByKey[key]; ok {
		return existing, errors.New("already exists")
	}

	user := &irodstypes.IRODSUser{
		ID:   int64(len(f.usersByKey) + 500),
		Name: username,
		Zone: zoneName,
		Type: userType,
	}
	f.usersByKey[key] = user
	return user, nil
}

func (f *catalogTestFileSystem) ChangeUserPassword(username string, zoneName string, _ string) error {
	if _, ok := f.usersByKey[catalogUserKey(username, zoneName)]; !ok {
		return irodstypes.NewUserNotFoundError(username)
	}
	return nil
}

func (f *catalogTestFileSystem) ChangeUserType(username string, zoneName string, newType irodstypes.IRODSUserType) error {
	user, ok := f.usersByKey[catalogUserKey(username, zoneName)]
	if !ok {
		return irodstypes.NewUserNotFoundError(username)
	}
	user.Type = newType
	return nil
}

func (f *catalogTestFileSystem) RemoveUser(username string, zoneName string, _ irodstypes.IRODSUserType) error {
	key := catalogUserKey(username, zoneName)
	if _, ok := f.usersByKey[key]; !ok {
		return irodstypes.NewUserNotFoundError(username)
	}
	delete(f.usersByKey, key)
	delete(f.groupMembers, key)
	for groupKey, members := range f.groupMembers {
		filtered := members[:0]
		for _, member := range members {
			if member == username {
				continue
			}
			filtered = append(filtered, member)
		}
		f.groupMembers[groupKey] = filtered
	}
	return nil
}

func (f *catalogTestFileSystem) AddGroupMember(groupName string, username string, zoneName string) error {
	group, ok := f.usersByKey[catalogUserKey(groupName, zoneName)]
	if !ok || group.Type != irodstypes.IRODSUserRodsGroup {
		return irodstypes.NewUserNotFoundError(groupName)
	}
	if _, ok := f.usersByKey[catalogUserKey(username, zoneName)]; !ok {
		return irodstypes.NewUserNotFoundError(username)
	}

	key := catalogUserKey(groupName, zoneName)
	members := f.groupMembers[key]
	for _, member := range members {
		if member == username {
			return nil
		}
	}
	f.groupMembers[key] = append(members, username)
	return nil
}

func (f *catalogTestFileSystem) RemoveGroupMember(groupName string, username string, zoneName string) error {
	key := catalogUserKey(groupName, zoneName)
	members, ok := f.groupMembers[key]
	if !ok {
		return irodstypes.NewUserNotFoundError(groupName)
	}

	filtered := members[:0]
	removed := false
	for _, member := range members {
		if member == username {
			removed = true
			continue
		}
		filtered = append(filtered, member)
	}
	if !removed {
		return irodstypes.NewUserNotFoundError(username)
	}
	f.groupMembers[key] = filtered
	return nil
}

func (f *catalogTestFileSystem) Release() {
	f.released = true
}

func (f *catalogTestFileSystem) GetTicket(ticketName string) (*irodstypes.IRODSTicket, error) {
	ticket, ok := f.ticketsByName[ticketName]
	if !ok {
		return nil, irodstypes.NewTicketNotFoundError(ticketName)
	}
	return ticket, nil
}

func (f *catalogTestFileSystem) ListTickets() ([]*irodstypes.IRODSTicket, error) {
	results := make([]*irodstypes.IRODSTicket, 0, len(f.ticketsByName))
	for _, ticket := range f.ticketsByName {
		results = append(results, ticket)
	}
	return results, nil
}

func (f *catalogTestFileSystem) CreateTicket(ticketName string, ticketType irodstypes.TicketType, irodsPath string) error {
	if _, ok := f.entriesByPath[irodsPath]; !ok {
		return irodstypes.NewFileNotFoundError(irodsPath)
	}

	f.ticketsByName[ticketName] = &irodstypes.IRODSTicket{
		ID:         int64(len(f.ticketsByName) + 1000),
		Name:       ticketName,
		Type:       ticketType,
		Owner:      "alice",
		OwnerZone:  "tempZone",
		ObjectType: "data",
		Path:       irodsPath,
	}
	return nil
}

func (f *catalogTestFileSystem) DeleteTicket(ticketName string) error {
	if _, ok := f.ticketsByName[ticketName]; !ok {
		return irodstypes.NewTicketNotFoundError(ticketName)
	}
	delete(f.ticketsByName, ticketName)
	return nil
}

func (f *catalogTestFileSystem) ModifyTicketUseLimit(ticketName string, uses int64) error {
	ticket, ok := f.ticketsByName[ticketName]
	if !ok {
		return irodstypes.NewTicketNotFoundError(ticketName)
	}
	ticket.UsesLimit = uses
	return nil
}

func (f *catalogTestFileSystem) ClearTicketUseLimit(ticketName string) error {
	return f.ModifyTicketUseLimit(ticketName, 0)
}

func (f *catalogTestFileSystem) ModifyTicketExpirationTime(ticketName string, expirationTime time.Time) error {
	ticket, ok := f.ticketsByName[ticketName]
	if !ok {
		return irodstypes.NewTicketNotFoundError(ticketName)
	}
	ticket.ExpirationTime = expirationTime
	return nil
}

func (f *catalogTestFileSystem) ClearTicketExpirationTime(ticketName string) error {
	return f.ModifyTicketExpirationTime(ticketName, time.Time{})
}

func (f *catalogTestFileSystem) removeDirRecursive(irodsPath string) {
	for _, child := range f.childrenByPath[irodsPath] {
		if child == nil {
			continue
		}
		if child.IsDir() {
			f.removeDirRecursive(child.Path)
			continue
		}
		delete(f.entriesByPath, child.Path)
		delete(f.contentByPath, child.Path)
		delete(f.metadataByPath, child.Path)
	}

	delete(f.childrenByPath, irodsPath)
	delete(f.entriesByPath, irodsPath)
	delete(f.metadataByPath, irodsPath)

	parentPath := path.Dir(irodsPath)
	if parentPath != "." && parentPath != "/" && parentPath != irodsPath {
		f.childrenByPath[parentPath] = filterCatalogChildEntry(f.childrenByPath[parentPath], irodsPath)
	}
}

func (f *catalogTestFileSystem) renameDirRecursive(srcPath string, destPath string) {
	entry := f.entriesByPath[srcPath]
	parentSrc := path.Dir(srcPath)
	parentDest := path.Dir(destPath)

	entry.Path = destPath
	entry.Name = path.Base(destPath)
	f.entriesByPath[destPath] = entry
	delete(f.entriesByPath, srcPath)

	children := f.childrenByPath[srcPath]
	delete(f.childrenByPath, srcPath)
	f.childrenByPath[destPath] = children

	if metas, ok := f.metadataByPath[srcPath]; ok {
		f.metadataByPath[destPath] = metas
		delete(f.metadataByPath, srcPath)
	}

	f.childrenByPath[parentSrc] = filterCatalogChildEntry(f.childrenByPath[parentSrc], srcPath)
	f.childrenByPath[parentDest] = append(f.childrenByPath[parentDest], entry)

	for _, child := range children {
		if child == nil {
			continue
		}
		childDest := path.Join(destPath, path.Base(child.Path))
		if child.IsDir() {
			f.renameDirRecursive(child.Path, childDest)
			continue
		}
		f.renameFileWithinDir(child.Path, childDest)
	}
}

func (f *catalogTestFileSystem) renameFileWithinDir(srcPath string, destPath string) {
	entry := f.entriesByPath[srcPath]
	entry.Path = destPath
	entry.Name = path.Base(destPath)
	f.entriesByPath[destPath] = entry
	delete(f.entriesByPath, srcPath)

	if data, ok := f.contentByPath[srcPath]; ok {
		f.contentByPath[destPath] = data
		delete(f.contentByPath, srcPath)
	}
	if metas, ok := f.metadataByPath[srcPath]; ok {
		f.metadataByPath[destPath] = metas
		delete(f.metadataByPath, srcPath)
	}
}

func filterCatalogChildEntry(entries []*irodsfs.Entry, targetPath string) []*irodsfs.Entry {
	filtered := entries[:0]
	for _, entry := range entries {
		if entry == nil || path.Clean(entry.Path) == targetPath {
			continue
		}
		filtered = append(filtered, entry)
	}
	return filtered
}

func catalogUserKey(username string, zone string) string {
	return strings.TrimSpace(zone) + "/" + strings.TrimSpace(username)
}

type catalogTestFileHandle struct {
	reader  *bytes.Reader
	writer  *bytes.Buffer
	onClose func([]byte)
}

func (f *catalogTestFileHandle) ReadAt(buffer []byte, offset int64) (int, error) {
	return f.reader.ReadAt(buffer, offset)
}

func (f *catalogTestFileHandle) Write(data []byte) (int, error) {
	if f.writer == nil {
		return 0, errors.New("file handle is not writable")
	}

	return f.writer.Write(data)
}

func (f *catalogTestFileHandle) Close() error {
	if f.onClose != nil && f.writer != nil {
		f.onClose(f.writer.Bytes())
	}
	return nil
}

func (f *catalogTestFileSystem) ListResources() ([]*irodstypes.IRODSResource, error) {
	return f.resources, nil
}

func (f *catalogTestFileSystem) GetResource(resourceName string) (*irodstypes.IRODSResource, error) {
	for _, resource := range f.resources {
		if resource != nil && resource.Name == resourceName {
			return resource, nil
		}
	}
	return nil, irodstypes.NewResourceNotFoundError(resourceName)
}
