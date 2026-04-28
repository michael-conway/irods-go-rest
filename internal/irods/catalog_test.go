package irods

import (
	"bytes"
	"context"
	"errors"
	"io"
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
	ticketsByName  map[string]*irodstypes.IRODSTicket
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

type catalogTestFileHandle struct {
	reader *bytes.Reader
}

func (f *catalogTestFileHandle) ReadAt(buffer []byte, offset int64) (int, error) {
	return f.reader.ReadAt(buffer, offset)
}

func (f *catalogTestFileHandle) Close() error {
	return nil
}
