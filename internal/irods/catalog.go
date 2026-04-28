package irods

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"mime"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	irodsfs "github.com/cyverse/go-irodsclient/fs"
	irodscommon "github.com/cyverse/go-irodsclient/irods/common"
	irodslibfs "github.com/cyverse/go-irodsclient/irods/fs"
	irodstypes "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/michael-conway/irods-go-rest/internal/config"
	"github.com/michael-conway/irods-go-rest/internal/domain"
	"github.com/michael-conway/irods-go-rest/internal/logutil"
)

var ErrNotFound = errors.New("resource not found")
var ErrPermissionDenied = errors.New("permission denied")
var ErrConflict = errors.New("conflict")

type RequestContext struct {
	AuthScheme    string
	Username      string
	BasicPassword string
	Ticket        string
}

type PathLookupOptions struct {
	VerboseLevel int
}

type PathCreateOptions struct {
	ChildName string
	Kind      string
	Mkdirs    bool
}

type CatalogService interface {
	GetPath(ctx context.Context, requestContext *RequestContext, absolutePath string, options PathLookupOptions) (domain.PathEntry, error)
	GetPathChildren(ctx context.Context, requestContext *RequestContext, absolutePath string) ([]domain.PathEntry, error)
	CreatePathChild(ctx context.Context, requestContext *RequestContext, absolutePath string, options PathCreateOptions) (domain.PathEntry, error)
	DeletePath(ctx context.Context, requestContext *RequestContext, absolutePath string, force bool) error
	RenamePath(ctx context.Context, requestContext *RequestContext, absolutePath string, newName string) (domain.PathEntry, error)
	GetPathMetadata(ctx context.Context, requestContext *RequestContext, absolutePath string) ([]domain.AVUMetadata, error)
	AddPathMetadata(ctx context.Context, requestContext *RequestContext, absolutePath string, attrib string, value string, unit string) (domain.AVUMetadata, error)
	UpdatePathMetadata(ctx context.Context, requestContext *RequestContext, absolutePath string, avuID string, attrib string, value string, unit string) (domain.AVUMetadata, error)
	DeletePathMetadata(ctx context.Context, requestContext *RequestContext, absolutePath string, avuID string) error
	GetPathChecksum(ctx context.Context, requestContext *RequestContext, absolutePath string) (domain.PathChecksum, error)
	ComputePathChecksum(ctx context.Context, requestContext *RequestContext, absolutePath string) (domain.PathChecksum, error)
	GetObjectContentByPath(ctx context.Context, requestContext *RequestContext, absolutePath string) (domain.ObjectContent, error)
}

type CatalogFileSystem interface {
	Stat(irodsPath string) (*irodsfs.Entry, error)
	List(irodsPath string) ([]*irodsfs.Entry, error)
	MakeDir(irodsPath string, recurse bool) error
	CreateFile(irodsPath string, resource string, mode string) (CatalogFileHandle, error)
	RemoveDir(irodsPath string, recurse bool, force bool) error
	RemoveFile(irodsPath string, force bool) error
	RenameDir(srcPath string, destPath string) error
	RenameFile(srcPath string, destPath string) error
	ListMetadata(irodsPath string) ([]*irodstypes.IRODSMeta, error)
	AddMetadata(irodsPath string, attName string, attValue string, attUnits string) error
	DeleteMetadata(irodsPath string, avuID int64) error
	ComputeChecksum(irodsPath string, resource string) (*irodstypes.IRODSChecksum, error)
	OpenFile(irodsPath string, resource string, mode string) (CatalogFileHandle, error)
	GetTicket(ticketName string) (*irodstypes.IRODSTicket, error)
	ListTickets() ([]*irodstypes.IRODSTicket, error)
	CreateTicket(ticketName string, ticketType irodstypes.TicketType, path string) error
	DeleteTicket(ticketName string) error
	ModifyTicketUseLimit(ticketName string, uses int64) error
	ClearTicketUseLimit(ticketName string) error
	ModifyTicketExpirationTime(ticketName string, expirationTime time.Time) error
	ClearTicketExpirationTime(ticketName string) error
	Release()
}

type CatalogFileHandle interface {
	ReadAt(buffer []byte, offset int64) (int, error)
	Close() error
}

type CatalogFileSystemFactory func(account *irodstypes.IRODSAccount, applicationName string) (CatalogFileSystem, error)

type catalogService struct {
	cfg              config.RestConfig
	createFileSystem CatalogFileSystemFactory
}

func NewCatalogService(cfg config.RestConfig) CatalogService {
	return NewCatalogServiceWithFactory(cfg, func(account *irodstypes.IRODSAccount, applicationName string) (CatalogFileSystem, error) {
		filesystem, err := irodsfs.NewFileSystemWithDefault(account, applicationName)
		if err != nil {
			return nil, err
		}

		return &catalogFileSystemAdapter{filesystem: filesystem}, nil
	})
}

func NewCatalogServiceWithFactory(cfg config.RestConfig, factory CatalogFileSystemFactory) CatalogService {
	return &catalogService{
		cfg:              cfg,
		createFileSystem: factory,
	}
}

func (s *catalogService) GetPath(_ context.Context, requestContext *RequestContext, absolutePath string, options PathLookupOptions) (domain.PathEntry, error) {
	absolutePath = strings.TrimSpace(absolutePath)
	if absolutePath == "" {
		return domain.PathEntry{}, fmt.Errorf("%w: path %q", ErrNotFound, absolutePath)
	}

	slog.Debug("catalog GetPath start", "path", absolutePath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))

	filesystem, err := s.filesystemForRequest(requestContext, "irods-go-rest-get-path")
	if err != nil {
		logIRODSError("catalog GetPath filesystem setup failed", err, "path", absolutePath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.PathEntry{}, err
	}
	defer filesystem.Release()

	entry, err := filesystem.Stat(absolutePath)
	if err != nil {
		logIRODSError("catalog GetPath stat failed", err, "path", absolutePath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.PathEntry{}, normalizePathAccessError("stat path", absolutePath, err)
	}

	metadata, err := filesystem.ListMetadata(absolutePath)
	if err != nil {
		logIRODSError("catalog GetPath list metadata failed", err, "path", absolutePath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.PathEntry{}, normalizePathAccessError("list metadata", absolutePath, err)
	}

	if entry.IsDir() {
		children, err := filesystem.List(absolutePath)
		if err != nil {
			logIRODSError("catalog GetPath list children failed", err, "path", absolutePath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
			return domain.PathEntry{}, normalizePathAccessError("list children", absolutePath, err)
		}

		return collectionPathEntry(s.cfg.IrodsZone, entry, metadata, len(children), options), nil
	}

	return dataObjectPathEntry(s.cfg.IrodsZone, entry, metadata, options), nil
}

func (s *catalogService) GetPathChildren(_ context.Context, requestContext *RequestContext, absolutePath string) ([]domain.PathEntry, error) {
	absolutePath = strings.TrimSpace(absolutePath)
	if absolutePath == "" {
		return nil, fmt.Errorf("%w: path %q", ErrNotFound, absolutePath)
	}

	slog.Debug("catalog GetPathChildren start", "path", absolutePath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))

	filesystem, err := s.filesystemForRequest(requestContext, "irods-go-rest-get-path-children")
	if err != nil {
		logIRODSError("catalog GetPathChildren filesystem setup failed", err, "path", absolutePath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return nil, err
	}
	defer filesystem.Release()

	entry, err := filesystem.Stat(absolutePath)
	if err != nil {
		logIRODSError("catalog GetPathChildren stat failed", err, "path", absolutePath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return nil, normalizePathAccessError("stat path", absolutePath, err)
	}

	if !entry.IsDir() {
		return nil, fmt.Errorf("%w: path %q is not a collection", ErrNotFound, absolutePath)
	}

	children, err := filesystem.List(absolutePath)
	if err != nil {
		logIRODSError("catalog GetPathChildren list failed", err, "path", absolutePath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return nil, normalizePathAccessError("list children", absolutePath, err)
	}

	results := make([]domain.PathEntry, 0, len(children))
	for _, child := range children {
		if child == nil {
			continue
		}

		if child.IsDir() {
			results = append(results, collectionPathEntry(s.cfg.IrodsZone, child, nil, 0, PathLookupOptions{}))
			continue
		}

		results = append(results, dataObjectPathEntry(s.cfg.IrodsZone, child, nil, PathLookupOptions{}))
	}

	return results, nil
}

func (s *catalogService) CreatePathChild(_ context.Context, requestContext *RequestContext, absolutePath string, options PathCreateOptions) (domain.PathEntry, error) {
	absolutePath = strings.TrimSpace(absolutePath)
	if absolutePath == "" {
		return domain.PathEntry{}, fmt.Errorf("%w: path %q", ErrNotFound, absolutePath)
	}

	childName := strings.TrimSpace(options.ChildName)
	kind := strings.TrimSpace(options.Kind)
	if childName == "" {
		return domain.PathEntry{}, fmt.Errorf("child_name is required")
	}
	if kind != "collection" && kind != "data_object" {
		return domain.PathEntry{}, fmt.Errorf("kind must be collection or data_object")
	}
	if options.Mkdirs && kind != "collection" {
		return domain.PathEntry{}, fmt.Errorf("mkdirs is only supported for collection creation")
	}

	childPath, err := resolveChildPath(absolutePath, childName)
	if err != nil {
		return domain.PathEntry{}, err
	}

	slog.Debug("catalog CreatePathChild start", "path", absolutePath, "child_path", childPath, "kind", kind, "mkdirs", options.Mkdirs, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))

	filesystem, err := s.filesystemForRequest(requestContext, "irods-go-rest-create-path-child")
	if err != nil {
		logIRODSError("catalog CreatePathChild filesystem setup failed", err, "path", absolutePath, "child_path", childPath, "kind", kind, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.PathEntry{}, err
	}
	defer filesystem.Release()

	parentEntry, err := filesystem.Stat(absolutePath)
	if err != nil {
		logIRODSError("catalog CreatePathChild parent stat failed", err, "path", absolutePath, "child_path", childPath, "kind", kind, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.PathEntry{}, normalizePathAccessError("stat path", absolutePath, err)
	}
	if !parentEntry.IsDir() {
		return domain.PathEntry{}, fmt.Errorf("%w: path %q is not a collection", ErrNotFound, absolutePath)
	}

	switch kind {
	case "collection":
		if err := filesystem.MakeDir(childPath, options.Mkdirs); err != nil {
			logIRODSError("catalog CreatePathChild mkdir failed", err, "path", absolutePath, "child_path", childPath, "mkdirs", options.Mkdirs, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
			return domain.PathEntry{}, normalizePathAccessError("create collection", childPath, err)
		}
	case "data_object":
		handle, err := filesystem.CreateFile(childPath, "", string(irodstypes.FileOpenModeWriteOnly))
		if err != nil {
			logIRODSError("catalog CreatePathChild create file failed", err, "path", absolutePath, "child_path", childPath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
			return domain.PathEntry{}, normalizePathAccessError("create data object", childPath, err)
		}
		if err := handle.Close(); err != nil {
			logIRODSError("catalog CreatePathChild close created file failed", err, "path", absolutePath, "child_path", childPath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
			return domain.PathEntry{}, normalizePathAccessError("close data object", childPath, err)
		}
	}

	entry, err := filesystem.Stat(childPath)
	if err != nil {
		logIRODSError("catalog CreatePathChild child stat failed", err, "path", absolutePath, "child_path", childPath, "kind", kind, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.PathEntry{}, normalizePathAccessError("stat path", childPath, err)
	}

	metadata, err := filesystem.ListMetadata(childPath)
	if err != nil {
		logIRODSError("catalog CreatePathChild child metadata failed", err, "path", absolutePath, "child_path", childPath, "kind", kind, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.PathEntry{}, normalizePathAccessError("list metadata", childPath, err)
	}

	if entry.IsDir() {
		children, err := filesystem.List(childPath)
		if err != nil {
			logIRODSError("catalog CreatePathChild child list failed", err, "path", absolutePath, "child_path", childPath, "kind", kind, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
			return domain.PathEntry{}, normalizePathAccessError("list children", childPath, err)
		}

		return collectionPathEntry(s.cfg.IrodsZone, entry, metadata, len(children), PathLookupOptions{}), nil
	}

	return dataObjectPathEntry(s.cfg.IrodsZone, entry, metadata, PathLookupOptions{}), nil
}

func (s *catalogService) DeletePath(_ context.Context, requestContext *RequestContext, absolutePath string, force bool) error {
	absolutePath = strings.TrimSpace(absolutePath)
	if absolutePath == "" {
		return fmt.Errorf("%w: path %q", ErrNotFound, absolutePath)
	}

	slog.Debug("catalog DeletePath start", "path", absolutePath, "force", force, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))

	filesystem, err := s.filesystemForRequest(requestContext, "irods-go-rest-delete-path")
	if err != nil {
		logIRODSError("catalog DeletePath filesystem setup failed", err, "path", absolutePath, "force", force, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return err
	}
	defer filesystem.Release()

	entry, err := filesystem.Stat(absolutePath)
	if err != nil {
		logIRODSError("catalog DeletePath stat failed", err, "path", absolutePath, "force", force, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return normalizePathAccessError("stat path", absolutePath, err)
	}

	if entry.IsDir() {
		if !force {
			children, err := filesystem.List(absolutePath)
			if err != nil {
				logIRODSError("catalog DeletePath list failed", err, "path", absolutePath, "force", force, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
				return normalizePathAccessError("list children", absolutePath, err)
			}
			if len(children) > 0 {
				return fmt.Errorf("%w: collection %q is not empty; pass force=true for recursive delete", ErrConflict, absolutePath)
			}
		}

		if err := filesystem.RemoveDir(absolutePath, force, force); err != nil {
			logIRODSError("catalog DeletePath remove dir failed", err, "path", absolutePath, "force", force, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
			return normalizePathAccessError("delete collection", absolutePath, err)
		}
		return nil
	}

	if err := filesystem.RemoveFile(absolutePath, force); err != nil {
		logIRODSError("catalog DeletePath remove file failed", err, "path", absolutePath, "force", force, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return normalizePathAccessError("delete data object", absolutePath, err)
	}
	return nil
}

func (s *catalogService) RenamePath(_ context.Context, requestContext *RequestContext, absolutePath string, newName string) (domain.PathEntry, error) {
	absolutePath = strings.TrimSpace(absolutePath)
	newName = strings.TrimSpace(newName)
	if absolutePath == "" {
		return domain.PathEntry{}, fmt.Errorf("%w: path %q", ErrNotFound, absolutePath)
	}
	if newName == "" {
		return domain.PathEntry{}, fmt.Errorf("new_name is required")
	}

	destPath, err := resolveRenameDestination(absolutePath, newName)
	if err != nil {
		return domain.PathEntry{}, err
	}

	slog.Debug("catalog RenamePath start", "path", absolutePath, "dest_path", destPath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))

	filesystem, err := s.filesystemForRequest(requestContext, "irods-go-rest-rename-path")
	if err != nil {
		logIRODSError("catalog RenamePath filesystem setup failed", err, "path", absolutePath, "dest_path", destPath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.PathEntry{}, err
	}
	defer filesystem.Release()

	entry, err := filesystem.Stat(absolutePath)
	if err != nil {
		logIRODSError("catalog RenamePath stat failed", err, "path", absolutePath, "dest_path", destPath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.PathEntry{}, normalizePathAccessError("stat path", absolutePath, err)
	}

	if entry.IsDir() {
		if err := filesystem.RenameDir(absolutePath, destPath); err != nil {
			logIRODSError("catalog RenamePath rename dir failed", err, "path", absolutePath, "dest_path", destPath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
			return domain.PathEntry{}, normalizePathAccessError("rename collection", absolutePath, err)
		}
	} else {
		if err := filesystem.RenameFile(absolutePath, destPath); err != nil {
			logIRODSError("catalog RenamePath rename file failed", err, "path", absolutePath, "dest_path", destPath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
			return domain.PathEntry{}, normalizePathAccessError("rename data object", absolutePath, err)
		}
	}

	renamedEntry, err := filesystem.Stat(destPath)
	if err != nil {
		logIRODSError("catalog RenamePath stat renamed failed", err, "path", absolutePath, "dest_path", destPath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.PathEntry{}, normalizePathAccessError("stat path", destPath, err)
	}

	metadata, err := filesystem.ListMetadata(destPath)
	if err != nil {
		logIRODSError("catalog RenamePath metadata failed", err, "path", absolutePath, "dest_path", destPath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.PathEntry{}, normalizePathAccessError("list metadata", destPath, err)
	}

	if renamedEntry.IsDir() {
		children, err := filesystem.List(destPath)
		if err != nil {
			logIRODSError("catalog RenamePath list children failed", err, "path", absolutePath, "dest_path", destPath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
			return domain.PathEntry{}, normalizePathAccessError("list children", destPath, err)
		}
		return collectionPathEntry(s.cfg.IrodsZone, renamedEntry, metadata, len(children), PathLookupOptions{}), nil
	}

	return dataObjectPathEntry(s.cfg.IrodsZone, renamedEntry, metadata, PathLookupOptions{}), nil
}

func (s *catalogService) GetPathMetadata(_ context.Context, requestContext *RequestContext, absolutePath string) ([]domain.AVUMetadata, error) {
	absolutePath = strings.TrimSpace(absolutePath)
	if absolutePath == "" {
		return nil, fmt.Errorf("%w: path %q", ErrNotFound, absolutePath)
	}

	slog.Debug("catalog GetPathMetadata start", "path", absolutePath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))

	filesystem, err := s.filesystemForRequest(requestContext, "irods-go-rest-get-path-metadata")
	if err != nil {
		logIRODSError("catalog GetPathMetadata filesystem setup failed", err, "path", absolutePath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return nil, err
	}
	defer filesystem.Release()

	if _, err := filesystem.Stat(absolutePath); err != nil {
		logIRODSError("catalog GetPathMetadata stat failed", err, "path", absolutePath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return nil, normalizePathAccessError("stat path", absolutePath, err)
	}

	metadata, err := filesystem.ListMetadata(absolutePath)
	if err != nil {
		logIRODSError("catalog GetPathMetadata list metadata failed", err, "path", absolutePath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return nil, normalizePathAccessError("list metadata", absolutePath, err)
	}

	return avuMetadataList(metadata), nil
}

func (s *catalogService) AddPathMetadata(_ context.Context, requestContext *RequestContext, absolutePath string, attrib string, value string, unit string) (domain.AVUMetadata, error) {
	absolutePath = strings.TrimSpace(absolutePath)
	attrib = strings.TrimSpace(attrib)
	value = strings.TrimSpace(value)
	unit = strings.TrimSpace(unit)
	if absolutePath == "" {
		return domain.AVUMetadata{}, fmt.Errorf("%w: path %q", ErrNotFound, absolutePath)
	}
	if attrib == "" || value == "" {
		return domain.AVUMetadata{}, fmt.Errorf("attrib and value are required")
	}

	slog.Debug("catalog AddPathMetadata start", "path", absolutePath, "attrib", attrib, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))

	filesystem, err := s.filesystemForRequest(requestContext, "irods-go-rest-add-path-metadata")
	if err != nil {
		logIRODSError("catalog AddPathMetadata filesystem setup failed", err, "path", absolutePath, "attrib", attrib, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.AVUMetadata{}, err
	}
	defer filesystem.Release()

	if _, err := filesystem.Stat(absolutePath); err != nil {
		logIRODSError("catalog AddPathMetadata stat failed", err, "path", absolutePath, "attrib", attrib, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.AVUMetadata{}, normalizePathAccessError("stat path", absolutePath, err)
	}

	if err := filesystem.AddMetadata(absolutePath, attrib, value, unit); err != nil {
		logIRODSError("catalog AddPathMetadata add failed", err, "path", absolutePath, "attrib", attrib, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.AVUMetadata{}, normalizePathAccessError("add metadata", absolutePath, err)
	}

	metadata, err := filesystem.ListMetadata(absolutePath)
	if err != nil {
		logIRODSError("catalog AddPathMetadata list metadata failed", err, "path", absolutePath, "attrib", attrib, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.AVUMetadata{}, normalizePathAccessError("list metadata", absolutePath, err)
	}

	created, ok := findLatestAVUMetadata(metadata, attrib, value, unit)
	if !ok {
		return domain.AVUMetadata{}, fmt.Errorf("metadata add completed but created AVU was not found for path %q", absolutePath)
	}

	return created, nil
}

func (s *catalogService) UpdatePathMetadata(_ context.Context, requestContext *RequestContext, absolutePath string, avuID string, attrib string, value string, unit string) (domain.AVUMetadata, error) {
	absolutePath = strings.TrimSpace(absolutePath)
	avuID = strings.TrimSpace(avuID)
	attrib = strings.TrimSpace(attrib)
	value = strings.TrimSpace(value)
	unit = strings.TrimSpace(unit)
	if absolutePath == "" {
		return domain.AVUMetadata{}, fmt.Errorf("%w: path %q", ErrNotFound, absolutePath)
	}
	if avuID == "" {
		return domain.AVUMetadata{}, fmt.Errorf("avu_id is required")
	}
	if attrib == "" || value == "" {
		return domain.AVUMetadata{}, fmt.Errorf("attrib and value are required")
	}
	avuIDInt, err := strconv.ParseInt(avuID, 10, 64)
	if err != nil || avuIDInt <= 0 {
		return domain.AVUMetadata{}, fmt.Errorf("invalid avu id %q", avuID)
	}

	slog.Debug("catalog UpdatePathMetadata start", "path", absolutePath, "avu_id", avuID, "attrib", attrib, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))

	filesystem, err := s.filesystemForRequest(requestContext, "irods-go-rest-update-path-metadata")
	if err != nil {
		logIRODSError("catalog UpdatePathMetadata filesystem setup failed", err, "path", absolutePath, "avu_id", avuID, "attrib", attrib, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.AVUMetadata{}, err
	}
	defer filesystem.Release()

	if _, err := filesystem.Stat(absolutePath); err != nil {
		logIRODSError("catalog UpdatePathMetadata stat failed", err, "path", absolutePath, "avu_id", avuID, "attrib", attrib, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.AVUMetadata{}, normalizePathAccessError("stat path", absolutePath, err)
	}

	metadata, err := filesystem.ListMetadata(absolutePath)
	if err != nil {
		logIRODSError("catalog UpdatePathMetadata list metadata failed", err, "path", absolutePath, "avu_id", avuID, "attrib", attrib, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.AVUMetadata{}, normalizePathAccessError("list metadata", absolutePath, err)
	}
	if _, ok := findAVUMetadataByID(metadata, avuID); !ok {
		return domain.AVUMetadata{}, fmt.Errorf("%w: avu %q on path %q", ErrNotFound, avuID, absolutePath)
	}

	if err := filesystem.DeleteMetadata(absolutePath, avuIDInt); err != nil {
		logIRODSError("catalog UpdatePathMetadata delete existing failed", err, "path", absolutePath, "avu_id", avuID, "attrib", attrib, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.AVUMetadata{}, normalizePathAccessError("delete metadata", absolutePath, err)
	}
	if err := filesystem.AddMetadata(absolutePath, attrib, value, unit); err != nil {
		logIRODSError("catalog UpdatePathMetadata add replacement failed", err, "path", absolutePath, "avu_id", avuID, "attrib", attrib, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.AVUMetadata{}, normalizePathAccessError("add metadata", absolutePath, err)
	}

	metadata, err = filesystem.ListMetadata(absolutePath)
	if err != nil {
		logIRODSError("catalog UpdatePathMetadata list metadata after update failed", err, "path", absolutePath, "avu_id", avuID, "attrib", attrib, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.AVUMetadata{}, normalizePathAccessError("list metadata", absolutePath, err)
	}

	updated, ok := findLatestAVUMetadata(metadata, attrib, value, unit)
	if !ok {
		return domain.AVUMetadata{}, fmt.Errorf("metadata update completed but updated AVU was not found for path %q", absolutePath)
	}

	return updated, nil
}

func (s *catalogService) DeletePathMetadata(_ context.Context, requestContext *RequestContext, absolutePath string, avuID string) error {
	absolutePath = strings.TrimSpace(absolutePath)
	avuID = strings.TrimSpace(avuID)
	if absolutePath == "" {
		return fmt.Errorf("%w: path %q", ErrNotFound, absolutePath)
	}
	if avuID == "" {
		return fmt.Errorf("avu_id is required")
	}

	avuIDInt, err := strconv.ParseInt(avuID, 10, 64)
	if err != nil || avuIDInt <= 0 {
		return fmt.Errorf("invalid avu id %q", avuID)
	}

	slog.Debug("catalog DeletePathMetadata start", "path", absolutePath, "avu_id", avuID, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))

	filesystem, err := s.filesystemForRequest(requestContext, "irods-go-rest-delete-path-metadata")
	if err != nil {
		logIRODSError("catalog DeletePathMetadata filesystem setup failed", err, "path", absolutePath, "avu_id", avuID, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return err
	}
	defer filesystem.Release()

	if _, err := filesystem.Stat(absolutePath); err != nil {
		logIRODSError("catalog DeletePathMetadata stat failed", err, "path", absolutePath, "avu_id", avuID, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return normalizePathAccessError("stat path", absolutePath, err)
	}

	metadata, err := filesystem.ListMetadata(absolutePath)
	if err != nil {
		logIRODSError("catalog DeletePathMetadata list metadata failed", err, "path", absolutePath, "avu_id", avuID, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return normalizePathAccessError("list metadata", absolutePath, err)
	}
	if _, ok := findAVUMetadataByID(metadata, avuID); !ok {
		return fmt.Errorf("%w: avu %q on path %q", ErrNotFound, avuID, absolutePath)
	}

	if err := filesystem.DeleteMetadata(absolutePath, avuIDInt); err != nil {
		logIRODSError("catalog DeletePathMetadata delete failed", err, "path", absolutePath, "avu_id", avuID, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return normalizePathAccessError("delete metadata", absolutePath, err)
	}

	return nil
}

func (s *catalogService) GetPathChecksum(_ context.Context, requestContext *RequestContext, absolutePath string) (domain.PathChecksum, error) {
	absolutePath = strings.TrimSpace(absolutePath)
	if absolutePath == "" {
		return domain.PathChecksum{}, fmt.Errorf("%w: object %q", ErrNotFound, absolutePath)
	}

	slog.Debug("catalog GetPathChecksum start", "path", absolutePath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))

	filesystem, err := s.filesystemForRequest(requestContext, "irods-go-rest-get-path-checksum")
	if err != nil {
		logIRODSError("catalog GetPathChecksum filesystem setup failed", err, "path", absolutePath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.PathChecksum{}, err
	}
	defer filesystem.Release()

	entry, err := filesystem.Stat(absolutePath)
	if err != nil {
		logIRODSError("catalog GetPathChecksum stat failed", err, "path", absolutePath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.PathChecksum{}, normalizePathAccessError("stat path", absolutePath, err)
	}

	if entry.IsDir() {
		return domain.PathChecksum{}, fmt.Errorf("%w: path %q is not a data object", ErrNotFound, absolutePath)
	}

	return pathChecksumFromEntry(entry), nil
}

func (s *catalogService) ComputePathChecksum(_ context.Context, requestContext *RequestContext, absolutePath string) (domain.PathChecksum, error) {
	absolutePath = strings.TrimSpace(absolutePath)
	if absolutePath == "" {
		return domain.PathChecksum{}, fmt.Errorf("%w: object %q", ErrNotFound, absolutePath)
	}

	slog.Debug("catalog ComputePathChecksum start", "path", absolutePath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))

	filesystem, err := s.filesystemForRequest(requestContext, "irods-go-rest-compute-path-checksum")
	if err != nil {
		logIRODSError("catalog ComputePathChecksum filesystem setup failed", err, "path", absolutePath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.PathChecksum{}, err
	}
	defer filesystem.Release()

	entry, err := filesystem.Stat(absolutePath)
	if err != nil {
		logIRODSError("catalog ComputePathChecksum stat failed", err, "path", absolutePath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.PathChecksum{}, normalizePathAccessError("stat path", absolutePath, err)
	}

	if entry.IsDir() {
		return domain.PathChecksum{}, fmt.Errorf("%w: path %q is not a data object", ErrNotFound, absolutePath)
	}

	checksum, err := filesystem.ComputeChecksum(absolutePath, "")
	if err != nil {
		logIRODSError("catalog ComputePathChecksum compute failed", err, "path", absolutePath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.PathChecksum{}, normalizePathAccessError("compute checksum", absolutePath, err)
	}

	return pathChecksumFromIRODSChecksum(checksum), nil
}

func (s *catalogService) GetObjectContentByPath(_ context.Context, requestContext *RequestContext, absolutePath string) (domain.ObjectContent, error) {
	absolutePath = strings.TrimSpace(absolutePath)
	if absolutePath == "" {
		return domain.ObjectContent{}, fmt.Errorf("%w: object %q", ErrNotFound, absolutePath)
	}

	slog.Debug("catalog GetObjectContentByPath start", "path", absolutePath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))

	filesystem, err := s.filesystemForRequest(requestContext, "irods-go-rest-get-path-contents")
	if err != nil {
		logIRODSError("catalog GetObjectContentByPath filesystem setup failed", err, "path", absolutePath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.ObjectContent{}, err
	}

	entry, err := filesystem.Stat(absolutePath)
	if err != nil {
		filesystem.Release()
		logIRODSError("catalog GetObjectContentByPath stat failed", err, "path", absolutePath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.ObjectContent{}, normalizePathAccessError("stat path", absolutePath, err)
	}

	if entry.IsDir() {
		filesystem.Release()
		return domain.ObjectContent{}, fmt.Errorf("%w: path %q is not a data object", ErrNotFound, absolutePath)
	}

	handle, err := filesystem.OpenFile(absolutePath, "", string(irodstypes.FileOpenModeReadOnly))
	if err != nil {
		filesystem.Release()
		logIRODSError("catalog GetObjectContentByPath open file failed", err, "path", absolutePath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.ObjectContent{}, normalizePathAccessError("open object", absolutePath, err)
	}

	return domain.ObjectContent{
		Path:        absolutePath,
		FileName:    filepath.Base(absolutePath),
		ContentType: inferredMimeType(entry),
		Size:        entry.Size,
		Checksum:    pathChecksumPointerFromEntry(entry),
		UpdatedAt:   timePointer(entry.ModifyTime),
		Reader: &catalogObjectReader{
			handle:     handle,
			filesystem: filesystem,
		},
	}, nil
}

func (s *catalogService) filesystemForRequest(requestContext *RequestContext, applicationName string) (CatalogFileSystem, error) {
	account, err := s.accountForRequest(requestContext)
	if err != nil {
		logIRODSError("catalog filesystemForRequest account creation failed", err, "application_name", applicationName, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return nil, err
	}

	filesystem, err := s.createFileSystem(account, applicationName)
	if err != nil {
		logIRODSError(
			"catalog filesystemForRequest connect failed",
			err,
			"application_name", applicationName,
			"auth_scheme", safeAuthScheme(requestContext),
			"irods_proxy_user", account.ProxyUser,
			"irods_client_user", account.ClientUser,
			"irods_zone", account.ClientZone,
			"irods_host", account.Host,
			"irods_port", account.Port,
		)
		return nil, fmt.Errorf("connect to iRODS: %w", err)
	}

	return filesystem, nil
}

func (s *catalogService) accountForRequest(requestContext *RequestContext) (*irodstypes.IRODSAccount, error) {
	if requestContext == nil {
		logIRODSError("catalog accountForRequest missing request context", fmt.Errorf("missing request context"))
		return nil, fmt.Errorf("missing request context")
	}

	switch requestContext.AuthScheme {
	case "basic":
		slog.Debug(
			"catalog resolved direct iRODS account",
			"http_auth_scheme", requestContext.AuthScheme,
			"irods_account_mode", "direct",
			"irods_proxy_user", requestContext.Username,
			"irods_client_user", requestContext.Username,
			"irods_zone", s.cfg.IrodsZone,
		)
		account, err := irodstypes.CreateIRODSAccount(
			s.cfg.IrodsHost,
			s.cfg.IrodsPort,
			requestContext.Username,
			s.cfg.IrodsZone,
			irodstypes.GetAuthScheme(s.cfg.IrodsAuthScheme),
			requestContext.BasicPassword,
			s.cfg.IrodsDefaultResource,
		)
		if err != nil {
			logIRODSError("catalog direct account creation failed", err, "http_auth_scheme", requestContext.AuthScheme, "irods_proxy_user", requestContext.Username, "irods_client_user", requestContext.Username, "irods_zone", s.cfg.IrodsZone)
			return nil, fmt.Errorf("create iRODS account: %w", err)
		}
		return account, nil
	case "bearer-ticket":
		slog.Debug(
			"catalog resolved ticket iRODS account",
			"http_auth_scheme", requestContext.AuthScheme,
			"irods_account_mode", "ticket",
			"irods_proxy_user", s.cfg.IrodsAdminUser,
			"irods_client_user", s.cfg.IrodsAdminUser,
			"irods_zone", s.cfg.IrodsZone,
		)
		account, err := irodstypes.CreateIRODSAccountForTicket(
			s.cfg.IrodsHost,
			s.cfg.IrodsPort,
			s.cfg.IrodsAdminUser,
			s.cfg.IrodsZone,
			irodstypes.GetAuthScheme(s.cfg.IrodsAuthScheme),
			s.cfg.IrodsAdminPassword,
			requestContext.Ticket,
			s.cfg.IrodsDefaultResource,
		)
		if err != nil {
			logIRODSError("catalog ticket account creation failed", err, "http_auth_scheme", requestContext.AuthScheme, "irods_proxy_user", s.cfg.IrodsAdminUser, "irods_client_user", s.cfg.IrodsAdminUser, "irods_zone", s.cfg.IrodsZone)
			return nil, fmt.Errorf("create iRODS ticket account: %w", err)
		}
		return account, nil
	case "bearer":
		slog.Debug(
			"catalog resolved proxy iRODS account",
			"http_auth_scheme", requestContext.AuthScheme,
			"irods_account_mode", "proxy",
			"irods_proxy_user", s.cfg.IrodsAdminUser,
			"irods_client_user", requestContext.Username,
			"irods_zone", s.cfg.IrodsZone,
		)
		account, err := irodstypes.CreateIRODSProxyAccount(
			s.cfg.IrodsHost,
			s.cfg.IrodsPort,
			requestContext.Username,
			s.cfg.IrodsZone,
			s.cfg.IrodsAdminUser,
			s.cfg.IrodsZone,
			irodstypes.GetAuthScheme(s.cfg.IrodsAuthScheme),
			s.cfg.IrodsAdminPassword,
			s.cfg.IrodsDefaultResource,
		)
		if err != nil {
			logIRODSError("catalog proxy account creation failed", err, "http_auth_scheme", requestContext.AuthScheme, "irods_proxy_user", s.cfg.IrodsAdminUser, "irods_client_user", requestContext.Username, "irods_zone", s.cfg.IrodsZone)
			return nil, fmt.Errorf("create iRODS proxy account: %w", err)
		}
		return account, nil
	default:
		logIRODSError("catalog unsupported auth scheme", fmt.Errorf("unsupported auth scheme %q", requestContext.AuthScheme), "http_auth_scheme", requestContext.AuthScheme, "username", requestContext.Username)
		return nil, fmt.Errorf("unsupported auth scheme %q", requestContext.AuthScheme)
	}
}

func logIRODSError(msg string, err error, args ...any) {
	logArgs := append([]any{"error", err.Error(), "stack_trace", logutil.StackTrace()}, args...)
	slog.Error(msg, logArgs...)
}

func safeAuthScheme(requestContext *RequestContext) string {
	if requestContext == nil {
		return ""
	}

	return requestContext.AuthScheme
}

func safeUsername(requestContext *RequestContext) string {
	if requestContext == nil {
		return ""
	}

	return requestContext.Username
}

type catalogObjectReader struct {
	handle     CatalogFileHandle
	filesystem CatalogFileSystem
}

type catalogFileSystemAdapter struct {
	filesystem *irodsfs.FileSystem
}

func (a *catalogFileSystemAdapter) Stat(irodsPath string) (*irodsfs.Entry, error) {
	return a.filesystem.Stat(irodsPath)
}

func (a *catalogFileSystemAdapter) List(irodsPath string) ([]*irodsfs.Entry, error) {
	return a.filesystem.List(irodsPath)
}

func (a *catalogFileSystemAdapter) MakeDir(irodsPath string, recurse bool) error {
	return a.filesystem.MakeDir(irodsPath, recurse)
}

func (a *catalogFileSystemAdapter) CreateFile(irodsPath string, resource string, mode string) (CatalogFileHandle, error) {
	return a.filesystem.CreateFile(irodsPath, resource, mode)
}

func (a *catalogFileSystemAdapter) RemoveDir(irodsPath string, recurse bool, force bool) error {
	return a.filesystem.RemoveDir(irodsPath, recurse, force)
}

func (a *catalogFileSystemAdapter) RemoveFile(irodsPath string, force bool) error {
	return a.filesystem.RemoveFile(irodsPath, force)
}

func (a *catalogFileSystemAdapter) RenameDir(srcPath string, destPath string) error {
	return a.filesystem.RenameDir(srcPath, destPath)
}

func (a *catalogFileSystemAdapter) RenameFile(srcPath string, destPath string) error {
	return a.filesystem.RenameFile(srcPath, destPath)
}

func (a *catalogFileSystemAdapter) ListMetadata(irodsPath string) ([]*irodstypes.IRODSMeta, error) {
	return a.filesystem.ListMetadata(irodsPath)
}

func (a *catalogFileSystemAdapter) AddMetadata(irodsPath string, attName string, attValue string, attUnits string) error {
	return a.filesystem.AddMetadata(irodsPath, attName, attValue, attUnits)
}

func (a *catalogFileSystemAdapter) DeleteMetadata(irodsPath string, avuID int64) error {
	return a.filesystem.DeleteMetadata(irodsPath, avuID)
}

func (a *catalogFileSystemAdapter) ComputeChecksum(irodsPath string, resource string) (*irodstypes.IRODSChecksum, error) {
	conn, err := a.filesystem.GetMetadataConnection(false)
	if err != nil {
		return nil, err
	}
	defer a.filesystem.ReturnMetadataConnection(conn) //nolint:errcheck

	return irodslibfs.GetDataObjectChecksum(conn, irodsPath, resource)
}

func (a *catalogFileSystemAdapter) OpenFile(irodsPath string, resource string, mode string) (CatalogFileHandle, error) {
	return a.filesystem.OpenFile(irodsPath, resource, mode)
}

func (a *catalogFileSystemAdapter) GetTicket(ticketName string) (*irodstypes.IRODSTicket, error) {
	return a.filesystem.GetTicket(ticketName)
}

func (a *catalogFileSystemAdapter) ListTickets() ([]*irodstypes.IRODSTicket, error) {
	return a.filesystem.ListTickets()
}

func (a *catalogFileSystemAdapter) CreateTicket(ticketName string, ticketType irodstypes.TicketType, path string) error {
	return a.filesystem.CreateTicket(ticketName, ticketType, path)
}

func (a *catalogFileSystemAdapter) DeleteTicket(ticketName string) error {
	return a.filesystem.DeleteTicket(ticketName)
}

func (a *catalogFileSystemAdapter) ModifyTicketUseLimit(ticketName string, uses int64) error {
	return a.filesystem.ModifyTicketUseLimit(ticketName, uses)
}

func (a *catalogFileSystemAdapter) ClearTicketUseLimit(ticketName string) error {
	return a.filesystem.ClearTicketUseLimit(ticketName)
}

func (a *catalogFileSystemAdapter) ModifyTicketExpirationTime(ticketName string, expirationTime time.Time) error {
	return a.filesystem.ModifyTicketExpirationTime(ticketName, expirationTime)
}

func (a *catalogFileSystemAdapter) ClearTicketExpirationTime(ticketName string) error {
	return a.filesystem.ClearTicketExpirationTime(ticketName)
}

func (a *catalogFileSystemAdapter) Release() {
	a.filesystem.Release()
}

func (r *catalogObjectReader) ReadAt(buffer []byte, offset int64) (int, error) {
	if r == nil || r.handle == nil {
		return 0, io.EOF
	}

	return r.handle.ReadAt(buffer, offset)
}

func (r *catalogObjectReader) Close() error {
	if r == nil {
		return nil
	}

	var closeErr error
	if r.handle != nil {
		closeErr = r.handle.Close()
	}
	if r.filesystem != nil {
		r.filesystem.Release()
	}
	return closeErr
}

func normalizePathAccessError(operation string, absolutePath string, err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, ErrNotFound) || errors.Is(err, ErrPermissionDenied) {
		return err
	}

	switch irodstypes.GetIRODSErrorCode(err) {
	case irodscommon.CAT_NO_ACCESS_PERMISSION, irodscommon.SYS_NO_API_PRIV:
		return fmt.Errorf("%w: path %q", ErrPermissionDenied, absolutePath)
	}

	if irodstypes.IsFileNotFoundError(err) {
		return fmt.Errorf("%w: path %q", ErrNotFound, absolutePath)
	}

	message := strings.ToLower(err.Error())
	if strings.Contains(message, "not found") || strings.Contains(message, "does not exist") {
		return fmt.Errorf("%w: path %q", ErrNotFound, absolutePath)
	}
	if strings.Contains(message, "no access permission") || strings.Contains(message, "permission denied") {
		return fmt.Errorf("%w: path %q", ErrPermissionDenied, absolutePath)
	}

	return fmt.Errorf("%s %q: %w", operation, absolutePath, err)
}

func resolveChildPath(parentPath string, childName string) (string, error) {
	parentPath = strings.TrimSpace(parentPath)
	childName = strings.TrimSpace(childName)
	if parentPath == "" {
		return "", fmt.Errorf("%w: path %q", ErrNotFound, parentPath)
	}
	if childName == "" {
		return "", fmt.Errorf("child_name is required")
	}
	if path.IsAbs(childName) {
		return "", fmt.Errorf("child_name must be relative to the parent path")
	}

	cleaned := path.Clean(childName)
	if cleaned == "." || cleaned == "" {
		return "", fmt.Errorf("child_name is required")
	}
	if cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", fmt.Errorf("child_name must remain within the parent path")
	}

	return path.Clean(path.Join(parentPath, cleaned)), nil
}

func resolveRenameDestination(sourcePath string, newName string) (string, error) {
	sourcePath = strings.TrimSpace(sourcePath)
	newName = strings.TrimSpace(newName)
	if sourcePath == "" {
		return "", fmt.Errorf("%w: path %q", ErrNotFound, sourcePath)
	}
	if newName == "" {
		return "", fmt.Errorf("new_name is required")
	}
	if path.IsAbs(newName) {
		return "", fmt.Errorf("new_name must not be an absolute path")
	}

	cleaned := path.Clean(newName)
	if cleaned == "." || cleaned == "" {
		return "", fmt.Errorf("new_name is required")
	}
	if strings.Contains(cleaned, "/") || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", fmt.Errorf("new_name must be a single path segment")
	}

	parentPath := path.Dir(path.Clean(sourcePath))
	if parentPath == "." || parentPath == "" {
		return "", fmt.Errorf("path %q cannot be renamed", sourcePath)
	}

	return path.Clean(path.Join(parentPath, cleaned)), nil
}

func metadataMap(metas []*irodstypes.IRODSMeta) map[string]string {
	if len(metas) == 0 {
		return nil
	}

	result := map[string]string{}
	for _, meta := range metas {
		if meta == nil {
			continue
		}

		name := strings.TrimSpace(meta.Name)
		if name == "" {
			continue
		}

		result[name] = strings.TrimSpace(meta.Value)
	}

	if len(result) == 0 {
		return nil
	}

	return result
}

func avuMetadataList(metas []*irodstypes.IRODSMeta) []domain.AVUMetadata {
	if len(metas) == 0 {
		return nil
	}

	result := make([]domain.AVUMetadata, 0, len(metas))
	for _, meta := range metas {
		if meta == nil {
			continue
		}

		result = append(result, domain.AVUMetadata{
			ID:        fmt.Sprintf("%d", meta.AVUID),
			Attrib:    strings.TrimSpace(meta.Name),
			Value:     strings.TrimSpace(meta.Value),
			Unit:      strings.TrimSpace(meta.Units),
			CreatedAt: timePointer(meta.CreateTime),
			UpdatedAt: timePointer(meta.ModifyTime),
		})
	}

	if len(result) == 0 {
		return nil
	}

	return result
}

func avuMetadataEntry(meta *irodstypes.IRODSMeta) domain.AVUMetadata {
	if meta == nil {
		return domain.AVUMetadata{}
	}

	return domain.AVUMetadata{
		ID:        fmt.Sprintf("%d", meta.AVUID),
		Attrib:    strings.TrimSpace(meta.Name),
		Value:     strings.TrimSpace(meta.Value),
		Unit:      strings.TrimSpace(meta.Units),
		CreatedAt: timePointer(meta.CreateTime),
		UpdatedAt: timePointer(meta.ModifyTime),
	}
}

func findLatestAVUMetadata(metas []*irodstypes.IRODSMeta, attrib string, value string, unit string) (domain.AVUMetadata, bool) {
	var selected *irodstypes.IRODSMeta
	for _, meta := range metas {
		if meta == nil {
			continue
		}
		if strings.TrimSpace(meta.Name) != attrib || strings.TrimSpace(meta.Value) != value || strings.TrimSpace(meta.Units) != unit {
			continue
		}
		if selected == nil || meta.AVUID > selected.AVUID {
			selected = meta
		}
	}
	if selected == nil {
		return domain.AVUMetadata{}, false
	}
	return avuMetadataEntry(selected), true
}

func findAVUMetadataByID(metas []*irodstypes.IRODSMeta, avuID string) (domain.AVUMetadata, bool) {
	for _, meta := range metas {
		if meta == nil {
			continue
		}
		if fmt.Sprintf("%d", meta.AVUID) == avuID {
			return avuMetadataEntry(meta), true
		}
	}
	return domain.AVUMetadata{}, false
}

func checksumString(entry *irodsfs.Entry) string {
	if entry == nil || len(entry.CheckSum) == 0 {
		return ""
	}

	checksum, err := irodstypes.MakeIRODSChecksumString(entry.CheckSumAlgorithm, entry.CheckSum)
	if err != nil {
		return string(entry.CheckSum)
	}

	return checksum
}

func inferredMimeType(entry *irodsfs.Entry) string {
	if entry == nil || entry.IsDir() {
		return ""
	}

	contentType := mime.TypeByExtension(strings.ToLower(filepath.Ext(entry.Path)))
	if contentType == "" {
		return "application/octet-stream"
	}

	return contentType
}

func firstReplicaResource(entry *irodsfs.Entry) string {
	if entry == nil || len(entry.IRODSReplicas) == 0 || entry.IRODSReplicas[0].ResourceName == "" {
		return ""
	}

	return entry.IRODSReplicas[0].ResourceName
}

func collectionPathEntry(zone string, entry *irodsfs.Entry, metadata []*irodstypes.IRODSMeta, childCount int, options PathLookupOptions) domain.PathEntry {
	return domain.PathEntry{
		ID:          entry.Path,
		Path:        entry.Path,
		Kind:        "collection",
		Zone:        zone,
		CreatedAt:   timePointer(entry.CreateTime),
		UpdatedAt:   timePointer(entry.ModifyTime),
		Replicas:    pathReplicas(entry, options),
		HasChildren: childCount > 0,
		ChildCount:  childCount,
		Metadata:    metadataMap(metadata),
	}
}

func dataObjectPathEntry(zone string, entry *irodsfs.Entry, metadata []*irodstypes.IRODSMeta, options PathLookupOptions) domain.PathEntry {
	return domain.PathEntry{
		ID:          entry.Path,
		Path:        entry.Path,
		Kind:        "data_object",
		Checksum:    pathChecksumPointerFromEntry(entry),
		MimeType:    inferredMimeType(entry),
		Size:        entry.Size,
		DisplaySize: humanReadableSize(entry.Size),
		Zone:        zone,
		Resource:    firstReplicaResource(entry),
		CreatedAt:   timePointer(entry.CreateTime),
		UpdatedAt:   timePointer(entry.ModifyTime),
		Replicas:    pathReplicas(entry, options),
		Metadata:    metadataMap(metadata),
	}
}

func pathChecksumPointerFromEntry(entry *irodsfs.Entry) *domain.PathChecksum {
	checksum := pathChecksumFromEntry(entry)
	if checksum.Checksum == "" && checksum.Type == "" {
		return nil
	}

	return &checksum
}

func pathChecksumFromEntry(entry *irodsfs.Entry) domain.PathChecksum {
	if entry == nil {
		return domain.PathChecksum{}
	}

	checksum := checksumString(entry)
	return domain.PathChecksum{
		Checksum: checksum,
		Type:     checksumTypeFromStringOrAlgorithm(checksum, entry.CheckSumAlgorithm),
	}
}

func pathChecksumFromIRODSChecksum(checksum *irodstypes.IRODSChecksum) domain.PathChecksum {
	if checksum == nil {
		return domain.PathChecksum{}
	}

	checksumString := irodsChecksumString(checksum)
	return domain.PathChecksum{
		Checksum: checksumString,
		Type:     checksumTypeFromStringOrAlgorithm(checksumString, checksum.Algorithm),
	}
}

func timePointer(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}

	ts := value.UTC()
	return &ts
}

func humanReadableSize(size int64) string {
	if size < 0 {
		return ""
	}

	units := []string{"B", "KB", "MB", "GB"}
	value := float64(size)
	unitIndex := 0
	for value >= 1024 && unitIndex < len(units)-1 {
		value /= 1024
		unitIndex++
	}

	if unitIndex == 0 {
		return fmt.Sprintf("%d %s", size, units[unitIndex])
	}

	rounded := math.Round(value*10) / 10
	if rounded == math.Trunc(rounded) {
		return fmt.Sprintf("%.0f %s", rounded, units[unitIndex])
	}

	return fmt.Sprintf("%.1f %s", rounded, units[unitIndex])
}

func pathReplicas(entry *irodsfs.Entry, options PathLookupOptions) []domain.PathReplica {
	if options.VerboseLevel <= 0 || entry == nil || entry.IsDir() || len(entry.IRODSReplicas) == 0 {
		return nil
	}

	replicas := make([]domain.PathReplica, 0, len(entry.IRODSReplicas))
	for _, replica := range entry.IRODSReplicas {
		pathReplica := domain.PathReplica{
			Number:            replica.Number,
			Owner:             strings.TrimSpace(replica.Owner),
			ResourceName:      strings.TrimSpace(replica.ResourceName),
			ResourceHierarchy: strings.TrimSpace(replica.ResourceHierarchy),
			Size:              entry.Size,
			DisplaySize:       humanReadableSize(entry.Size),
			UpdatedAt:         timePointer(replica.ModifyTime),
			Status:            strings.TrimSpace(replica.Status),
			StatusSymbol:      replicaStatusSymbol(replica.Status),
			StatusDescription: replicaStatusDescription(replica.Status),
		}

		if options.VerboseLevel >= 2 {
			pathReplica.Checksum = irodsChecksumString(replica.Checksum)
			pathReplica.DataType = strings.TrimSpace(entry.DataType)
			pathReplica.PhysicalPath = strings.TrimSpace(replica.Path)
		}

		replicas = append(replicas, pathReplica)
	}

	return replicas
}

func irodsChecksumString(checksum *irodstypes.IRODSChecksum) string {
	if checksum == nil {
		return ""
	}

	return strings.TrimSpace(checksum.IRODSChecksumString)
}

func checksumTypeFromStringOrAlgorithm(checksum string, algorithm irodstypes.ChecksumAlgorithm) string {
	checksum = strings.TrimSpace(checksum)
	if checksum != "" {
		if prefix, _, ok := strings.Cut(checksum, ":"); ok {
			return strings.TrimSpace(prefix)
		}
	}

	switch algorithm {
	case irodstypes.ChecksumAlgorithmSHA256:
		return "sha2"
	case irodstypes.ChecksumAlgorithmMD5:
		return "md5"
	default:
		return ""
	}
}

func replicaStatusSymbol(status string) string {
	switch normalizeReplicaStatus(status) {
	case "0", "stale":
		return "X"
	case "1", "good":
		return "&"
	case "2", "intermediate":
		return "?"
	case "3", "read-locked":
		return "?"
	case "4", "write-locked":
		return "?"
	default:
		return "?"
	}
}

func replicaStatusDescription(status string) string {
	switch normalizeReplicaStatus(status) {
	case "0", "stale":
		return "stale"
	case "1", "good":
		return "good"
	case "2", "intermediate":
		return "intermediate"
	case "3", "read-locked":
		return "read-locked"
	case "4", "write-locked":
		return "write-locked"
	default:
		return ""
	}
}

func normalizeReplicaStatus(status string) string {
	status = strings.TrimSpace(strings.ToLower(status))
	switch status {
	case "0", "1", "2", "3", "4":
		return status
	case "stale", "good", "intermediate", "read-locked", "read_locked", "write-locked", "write_locked":
		return strings.ReplaceAll(status, "_", "-")
	default:
		return status
	}
}
