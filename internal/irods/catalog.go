package irods

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"mime"
	"net/url"
	"path"
	"path/filepath"
	"sort"
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

type PathChildrenSearchScope string

const (
	PathChildrenSearchScopeChildren PathChildrenSearchScope = "children"
	PathChildrenSearchScopeSubtree  PathChildrenSearchScope = "subtree"
	PathChildrenSearchScopeAbsolute PathChildrenSearchScope = "absolute"
)

type PathChildrenListOptions struct {
	NamePattern   string
	SearchScope   PathChildrenSearchScope
	CaseSensitive bool
	Sort          string
	Order         string
	Limit         int
	Offset        int
}

type PathChildrenSearchResult struct {
	Children      []domain.PathEntry
	MatchedCount  int
	NamePattern   string
	SearchScope   PathChildrenSearchScope
	CaseSensitive bool
}

type PathCreateOptions struct {
	ChildName string
	Kind      string
	Mkdirs    bool
}

type PathContentsUploadOptions struct {
	FileName  string
	Content   io.Reader
	Checksum  bool
	Overwrite bool
}

type PathReplicaCreateOptions struct {
	Resource string
	Update   bool
}

type PathReplicaMoveOptions struct {
	SourceResource      string
	DestinationResource string
	Update              bool
	MinCopies           int
	MinAgeMinutes       int
}

type PathReplicaTrimOptions struct {
	Resource      string
	ReplicaIndex  *int64
	MinCopies     int
	MinAgeMinutes int
}

type PathRelocateOperation string

const (
	PathRelocateOperationMove PathRelocateOperation = "move"
	PathRelocateOperationCopy PathRelocateOperation = "copy"
)

const (
	favoritesAVUAttribute = "iRODS:Favorite"
	favoritesAVUUnit      = "iRODS:Favorite"
)

type PathRelocateOptions struct {
	Operation       PathRelocateOperation
	NewName         string
	DestinationPath string
}

type FavoritePayload struct {
	Name         string `json:"name"`
	AbsolutePath string `json:"absolute_path"`
}

type CatalogService interface {
	GetPath(ctx context.Context, requestContext *RequestContext, absolutePath string, options PathLookupOptions) (domain.PathEntry, error)
	GetPathChildren(ctx context.Context, requestContext *RequestContext, absolutePath string) ([]domain.PathEntry, error)
	SearchPathChildren(ctx context.Context, requestContext *RequestContext, absolutePath string, options PathChildrenListOptions) (PathChildrenSearchResult, error)
	GetPathReplicas(ctx context.Context, requestContext *RequestContext, absolutePath string, verboseLevel int) ([]domain.PathReplica, error)
	UploadPathContents(ctx context.Context, requestContext *RequestContext, absolutePath string, options PathContentsUploadOptions) (domain.PathContentsUploadResult, error)
	CreatePathChild(ctx context.Context, requestContext *RequestContext, absolutePath string, options PathCreateOptions) (domain.PathEntry, error)
	CreatePathReplica(ctx context.Context, requestContext *RequestContext, absolutePath string, options PathReplicaCreateOptions) ([]domain.PathReplica, error)
	MovePathReplica(ctx context.Context, requestContext *RequestContext, absolutePath string, options PathReplicaMoveOptions) ([]domain.PathReplica, error)
	TrimPathReplica(ctx context.Context, requestContext *RequestContext, absolutePath string, options PathReplicaTrimOptions) ([]domain.PathReplica, error)
	DeletePath(ctx context.Context, requestContext *RequestContext, absolutePath string, force bool) error
	RenamePath(ctx context.Context, requestContext *RequestContext, absolutePath string, newName string) (domain.PathEntry, error)
	RelocatePath(ctx context.Context, requestContext *RequestContext, absolutePath string, options PathRelocateOptions) (domain.PathEntry, error)
	GetPathMetadata(ctx context.Context, requestContext *RequestContext, absolutePath string) ([]domain.AVUMetadata, error)
	AddPathMetadata(ctx context.Context, requestContext *RequestContext, absolutePath string, attrib string, value string, unit string) (domain.AVUMetadata, error)
	UpdatePathMetadata(ctx context.Context, requestContext *RequestContext, absolutePath string, avuID string, attrib string, value string, unit string) (domain.AVUMetadata, error)
	DeletePathMetadata(ctx context.Context, requestContext *RequestContext, absolutePath string, avuID string) error
	GetPathACL(ctx context.Context, requestContext *RequestContext, absolutePath string) (domain.PathACL, error)
	AddPathACL(ctx context.Context, requestContext *RequestContext, absolutePath string, acl irodstypes.IRODSAccess, recursive bool) (domain.PathACLEntry, error)
	UpdatePathACL(ctx context.Context, requestContext *RequestContext, absolutePath string, aclID string, accessLevel string, recursive bool) (domain.PathACLEntry, error)
	DeletePathACL(ctx context.Context, requestContext *RequestContext, absolutePath string, aclID string) error
	SetPathACLInheritance(ctx context.Context, requestContext *RequestContext, absolutePath string, enabled bool, recursive bool) error
	GetPathChecksum(ctx context.Context, requestContext *RequestContext, absolutePath string) (domain.PathChecksum, error)
	ComputePathChecksum(ctx context.Context, requestContext *RequestContext, absolutePath string) (domain.PathChecksum, error)
	GetObjectContentByPath(ctx context.Context, requestContext *RequestContext, absolutePath string) (domain.ObjectContent, error)
	ListFavorites(ctx context.Context, requestContext *RequestContext) ([]domain.Favorite, error)
	AddFavorite(ctx context.Context, requestContext *RequestContext, name string, favoritePath string) (domain.Favorite, error)
	RenameFavorite(ctx context.Context, requestContext *RequestContext, favoritePath string, name string) (domain.Favorite, error)
	RemoveFavorite(ctx context.Context, requestContext *RequestContext, favoritePath string) error
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
	CopyFile(srcPath string, destPath string, force bool) error
	ListMetadata(irodsPath string) ([]*irodstypes.IRODSMeta, error)
	AddMetadata(irodsPath string, attName string, attValue string, attUnits string) error
	DeleteMetadata(irodsPath string, avuID int64) error
	ListACLs(irodsPath string) ([]*irodstypes.IRODSAccess, error)
	GetDirACLInheritance(path string) (*irodstypes.IRODSAccessInheritance, error)
	ChangeACLs(path string, access irodstypes.IRODSAccessLevelType, userName string, zoneName string, recurse bool, adminFlag bool) error
	ChangeDirACLInheritance(path string, inherit bool, recurse bool, adminFlag bool) error
	ComputeChecksum(irodsPath string, resource string) (*irodstypes.IRODSChecksum, error)
	GetServerVersion() (*irodstypes.IRODSVersion, error)
	OpenFile(irodsPath string, resource string, mode string) (CatalogFileHandle, error)
	ListResources() ([]*irodstypes.IRODSResource, error)
	GetResource(resourceName string) (*irodstypes.IRODSResource, error)
	GetUser(username string, zoneName string, userType irodstypes.IRODSUserType) (*irodstypes.IRODSUser, error)
	ListUsers(zoneName string, userType irodstypes.IRODSUserType) ([]*irodstypes.IRODSUser, error)
	ListGroupMembers(zoneName string, groupName string) ([]*irodstypes.IRODSUser, error)
	CreateUser(username string, zoneName string, userType irodstypes.IRODSUserType) (*irodstypes.IRODSUser, error)
	ChangeUserPassword(username string, zoneName string, newPassword string) error
	ChangeUserType(username string, zoneName string, newType irodstypes.IRODSUserType) error
	RemoveUser(username string, zoneName string, userType irodstypes.IRODSUserType) error
	AddGroupMember(groupName string, username string, zoneName string) error
	RemoveGroupMember(groupName string, username string, zoneName string) error
	GetTicket(ticketName string) (*irodstypes.IRODSTicket, error)
	ListTickets() ([]*irodstypes.IRODSTicket, error)
	CreateTicket(ticketName string, ticketType irodstypes.TicketType, path string) error
	DeleteTicket(ticketName string) error
	ModifyTicketUseLimit(ticketName string, uses int64) error
	ClearTicketUseLimit(ticketName string) error
	ModifyTicketExpirationTime(ticketName string, expirationTime time.Time) error
	ClearTicketExpirationTime(ticketName string) error
	ReplicateFile(irodsPath string, resource string, update bool) error
	TrimDataObject(irodsPath string, resource string, minCopies int, minAgeMinutes int) error
	Release()
}

type CatalogFileHandle interface {
	ReadAt(buffer []byte, offset int64) (int, error)
	Write(data []byte) (int, error)
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

func (s *catalogService) GetPathChildren(ctx context.Context, requestContext *RequestContext, absolutePath string) ([]domain.PathEntry, error) {
	result, err := s.SearchPathChildren(ctx, requestContext, absolutePath, PathChildrenListOptions{})
	if err != nil {
		return nil, err
	}

	return result.Children, nil
}

func (s *catalogService) SearchPathChildren(_ context.Context, requestContext *RequestContext, absolutePath string, options PathChildrenListOptions) (PathChildrenSearchResult, error) {
	absolutePath = strings.TrimSpace(absolutePath)
	if absolutePath == "" {
		return PathChildrenSearchResult{}, fmt.Errorf("%w: path %q", ErrNotFound, absolutePath)
	}

	slog.Debug("catalog SearchPathChildren start", "path", absolutePath, "name_pattern", options.NamePattern, "search_scope", options.SearchScope, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))

	filesystem, err := s.filesystemForRequest(requestContext, "irods-go-rest-get-path-children")
	if err != nil {
		logIRODSError("catalog SearchPathChildren filesystem setup failed", err, "path", absolutePath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return PathChildrenSearchResult{}, err
	}
	defer filesystem.Release()

	entry, err := filesystem.Stat(absolutePath)
	if err != nil {
		logIRODSError("catalog SearchPathChildren stat failed", err, "path", absolutePath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return PathChildrenSearchResult{}, normalizePathAccessError("stat path", absolutePath, err)
	}

	if !entry.IsDir() {
		return PathChildrenSearchResult{}, fmt.Errorf("%w: path %q is not a collection", ErrNotFound, absolutePath)
	}

	normalized := normalizePathChildrenListOptions(options)
	searchRootPath := absolutePath
	switch normalized.SearchScope {
	case PathChildrenSearchScopeAbsolute:
		if strings.TrimSpace(s.cfg.IrodsZone) != "" {
			searchRootPath = "/" + strings.TrimSpace(s.cfg.IrodsZone)
		} else {
			searchRootPath = "/"
		}
	}

	entries, err := collectPathChildrenEntries(filesystem, searchRootPath, normalized.SearchScope != PathChildrenSearchScopeChildren)
	if err != nil {
		logIRODSError("catalog SearchPathChildren list failed", err, "path", searchRootPath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return PathChildrenSearchResult{}, normalizePathAccessError("list children", searchRootPath, err)
	}

	filteredEntries, err := filterPathChildrenEntries(entries, normalized)
	if err != nil {
		return PathChildrenSearchResult{}, err
	}

	sortPathChildrenEntries(filteredEntries, normalized.Sort, normalized.Order)
	matchedCount := len(filteredEntries)
	pagedEntries := paginatePathChildrenEntries(filteredEntries, normalized.Offset, normalized.Limit)
	results := mapPathChildrenEntries(s.cfg.IrodsZone, pagedEntries)

	return PathChildrenSearchResult{
		Children:      results,
		MatchedCount:  matchedCount,
		NamePattern:   normalized.NamePattern,
		SearchScope:   normalized.SearchScope,
		CaseSensitive: normalized.CaseSensitive,
	}, nil
}

func (s *catalogService) GetPathReplicas(_ context.Context, requestContext *RequestContext, absolutePath string, verboseLevel int) ([]domain.PathReplica, error) {
	absolutePath = strings.TrimSpace(absolutePath)
	if absolutePath == "" {
		return nil, fmt.Errorf("%w: path %q", ErrNotFound, absolutePath)
	}

	slog.Debug("catalog GetPathReplicas start", "path", absolutePath, "verbose_level", verboseLevel, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))

	filesystem, err := s.filesystemForRequest(requestContext, "irods-go-rest-get-path-replicas")
	if err != nil {
		logIRODSError("catalog GetPathReplicas filesystem setup failed", err, "path", absolutePath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return nil, err
	}
	defer filesystem.Release()

	entry, err := filesystem.Stat(absolutePath)
	if err != nil {
		logIRODSError("catalog GetPathReplicas stat failed", err, "path", absolutePath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return nil, normalizePathAccessError("stat path", absolutePath, err)
	}
	if entry.IsDir() {
		return nil, fmt.Errorf("%w: path %q is not a data object", ErrNotFound, absolutePath)
	}

	if verboseLevel < 1 {
		verboseLevel = 1
	}
	if verboseLevel > 2 {
		verboseLevel = 2
	}

	return pathReplicas(entry, PathLookupOptions{VerboseLevel: verboseLevel}), nil
}

func (s *catalogService) CreatePathReplica(_ context.Context, requestContext *RequestContext, absolutePath string, options PathReplicaCreateOptions) ([]domain.PathReplica, error) {
	absolutePath = strings.TrimSpace(absolutePath)
	resource := strings.TrimSpace(options.Resource)
	if absolutePath == "" {
		return nil, fmt.Errorf("%w: path %q", ErrNotFound, absolutePath)
	}
	if resource == "" {
		return nil, fmt.Errorf("resource is required")
	}

	slog.Debug("catalog CreatePathReplica start", "path", absolutePath, "resource", resource, "update", options.Update, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))

	filesystem, err := s.filesystemForRequest(requestContext, "irods-go-rest-create-path-replica")
	if err != nil {
		logIRODSError("catalog CreatePathReplica filesystem setup failed", err, "path", absolutePath, "resource", resource, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return nil, err
	}
	defer filesystem.Release()

	entry, err := filesystem.Stat(absolutePath)
	if err != nil {
		logIRODSError("catalog CreatePathReplica stat failed", err, "path", absolutePath, "resource", resource, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return nil, normalizePathAccessError("stat path", absolutePath, err)
	}
	if entry.IsDir() {
		return nil, fmt.Errorf("%w: path %q is not a data object", ErrNotFound, absolutePath)
	}

	if err := filesystem.ReplicateFile(absolutePath, resource, options.Update); err != nil {
		logIRODSError("catalog CreatePathReplica replicate failed", err, "path", absolutePath, "resource", resource, "update", options.Update, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return nil, normalizePathAccessError("replicate data object", absolutePath, err)
	}

	entry, err = filesystem.Stat(absolutePath)
	if err != nil {
		logIRODSError("catalog CreatePathReplica verify stat failed", err, "path", absolutePath, "resource", resource, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return nil, normalizePathAccessError("stat path", absolutePath, err)
	}

	return pathReplicas(entry, PathLookupOptions{VerboseLevel: 2}), nil
}

func (s *catalogService) MovePathReplica(_ context.Context, requestContext *RequestContext, absolutePath string, options PathReplicaMoveOptions) ([]domain.PathReplica, error) {
	absolutePath = strings.TrimSpace(absolutePath)
	sourceResource := strings.TrimSpace(options.SourceResource)
	destinationResource := strings.TrimSpace(options.DestinationResource)
	if absolutePath == "" {
		return nil, fmt.Errorf("%w: path %q", ErrNotFound, absolutePath)
	}
	if sourceResource == "" {
		return nil, fmt.Errorf("source_resource is required")
	}
	if destinationResource == "" {
		return nil, fmt.Errorf("destination_resource is required")
	}
	if sourceResource == destinationResource {
		return nil, fmt.Errorf("source_resource and destination_resource must differ")
	}

	minCopies := options.MinCopies
	if minCopies <= 0 {
		minCopies = 1
	}
	minAgeMinutes := options.MinAgeMinutes
	if minAgeMinutes < 0 {
		minAgeMinutes = 0
	}

	slog.Debug("catalog MovePathReplica start", "path", absolutePath, "source_resource", sourceResource, "destination_resource", destinationResource, "update", options.Update, "min_copies", minCopies, "min_age_minutes", minAgeMinutes, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))

	filesystem, err := s.filesystemForRequest(requestContext, "irods-go-rest-move-path-replica")
	if err != nil {
		logIRODSError("catalog MovePathReplica filesystem setup failed", err, "path", absolutePath, "source_resource", sourceResource, "destination_resource", destinationResource, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return nil, err
	}
	defer filesystem.Release()

	entry, err := filesystem.Stat(absolutePath)
	if err != nil {
		logIRODSError("catalog MovePathReplica stat failed", err, "path", absolutePath, "source_resource", sourceResource, "destination_resource", destinationResource, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return nil, normalizePathAccessError("stat path", absolutePath, err)
	}
	if entry.IsDir() {
		return nil, fmt.Errorf("%w: path %q is not a data object", ErrNotFound, absolutePath)
	}
	if !entryHasReplicaInResource(entry, sourceResource) {
		return nil, fmt.Errorf("%w: replica on resource %q for path %q", ErrNotFound, sourceResource, absolutePath)
	}

	if err := filesystem.ReplicateFile(absolutePath, destinationResource, options.Update); err != nil {
		logIRODSError("catalog MovePathReplica replicate destination failed", err, "path", absolutePath, "source_resource", sourceResource, "destination_resource", destinationResource, "update", options.Update, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return nil, normalizePathAccessError("replicate data object", absolutePath, err)
	}

	if err := filesystem.TrimDataObject(absolutePath, sourceResource, minCopies, minAgeMinutes); err != nil {
		logIRODSError("catalog MovePathReplica trim source failed", err, "path", absolutePath, "source_resource", sourceResource, "destination_resource", destinationResource, "min_copies", minCopies, "min_age_minutes", minAgeMinutes, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return nil, normalizePathAccessError("trim data object replica", absolutePath, err)
	}

	entry, err = filesystem.Stat(absolutePath)
	if err != nil {
		logIRODSError("catalog MovePathReplica verify stat failed", err, "path", absolutePath, "source_resource", sourceResource, "destination_resource", destinationResource, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return nil, normalizePathAccessError("stat path", absolutePath, err)
	}

	return pathReplicas(entry, PathLookupOptions{VerboseLevel: 2}), nil
}

func (s *catalogService) TrimPathReplica(_ context.Context, requestContext *RequestContext, absolutePath string, options PathReplicaTrimOptions) ([]domain.PathReplica, error) {
	absolutePath = strings.TrimSpace(absolutePath)
	if absolutePath == "" {
		return nil, fmt.Errorf("%w: path %q", ErrNotFound, absolutePath)
	}

	minCopies := options.MinCopies
	if minCopies <= 0 {
		minCopies = 1
	}
	minAgeMinutes := options.MinAgeMinutes
	if minAgeMinutes < 0 {
		minAgeMinutes = 0
	}

	slog.Debug("catalog TrimPathReplica start", "path", absolutePath, "resource", options.Resource, "replica_index", options.ReplicaIndex, "min_copies", minCopies, "min_age_minutes", minAgeMinutes, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))

	filesystem, err := s.filesystemForRequest(requestContext, "irods-go-rest-trim-path-replica")
	if err != nil {
		logIRODSError("catalog TrimPathReplica filesystem setup failed", err, "path", absolutePath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return nil, err
	}
	defer filesystem.Release()

	entry, err := filesystem.Stat(absolutePath)
	if err != nil {
		logIRODSError("catalog TrimPathReplica stat failed", err, "path", absolutePath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return nil, normalizePathAccessError("stat path", absolutePath, err)
	}
	if entry.IsDir() {
		return nil, fmt.Errorf("%w: path %q is not a data object", ErrNotFound, absolutePath)
	}

	resource := strings.TrimSpace(options.Resource)
	if resource == "" {
		resource, err = resolveReplicaResourceByIndex(entry, options.ReplicaIndex)
		if err != nil {
			return nil, err
		}
	}

	if err := filesystem.TrimDataObject(absolutePath, resource, minCopies, minAgeMinutes); err != nil {
		logIRODSError("catalog TrimPathReplica trim failed", err, "path", absolutePath, "resource", resource, "min_copies", minCopies, "min_age_minutes", minAgeMinutes, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return nil, normalizePathAccessError("trim data object replica", absolutePath, err)
	}

	entry, err = filesystem.Stat(absolutePath)
	if err != nil {
		logIRODSError("catalog TrimPathReplica verify stat failed", err, "path", absolutePath, "resource", resource, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return nil, normalizePathAccessError("stat path", absolutePath, err)
	}

	return pathReplicas(entry, PathLookupOptions{VerboseLevel: 2}), nil
}

func (s *catalogService) CreatePathChild(ctx context.Context, requestContext *RequestContext, absolutePath string, options PathCreateOptions) (domain.PathEntry, error) {
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

	verifyFS, entry, err := s.waitForObservedPath(ctx, requestContext, childPath, true, "irods-go-rest-create-path-child-verify")
	if err != nil {
		logIRODSError("catalog CreatePathChild child verification failed", err, "path", absolutePath, "child_path", childPath, "kind", kind, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.PathEntry{}, err
	}
	defer verifyFS.Release()

	metadata, err := verifyFS.ListMetadata(childPath)
	if err != nil {
		logIRODSError("catalog CreatePathChild child metadata failed", err, "path", absolutePath, "child_path", childPath, "kind", kind, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.PathEntry{}, normalizePathAccessError("list metadata", childPath, err)
	}

	if entry.IsDir() {
		children, err := verifyFS.List(childPath)
		if err != nil {
			logIRODSError("catalog CreatePathChild child list failed", err, "path", absolutePath, "child_path", childPath, "kind", kind, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
			return domain.PathEntry{}, normalizePathAccessError("list children", childPath, err)
		}

		return collectionPathEntry(s.cfg.IrodsZone, entry, metadata, len(children), PathLookupOptions{}), nil
	}

	return dataObjectPathEntry(s.cfg.IrodsZone, entry, metadata, PathLookupOptions{}), nil
}

func (s *catalogService) UploadPathContents(ctx context.Context, requestContext *RequestContext, absolutePath string, options PathContentsUploadOptions) (domain.PathContentsUploadResult, error) {
	absolutePath = strings.TrimSpace(absolutePath)
	if absolutePath == "" {
		return domain.PathContentsUploadResult{}, fmt.Errorf("%w: path %q", ErrNotFound, absolutePath)
	}

	fileName := strings.TrimSpace(options.FileName)
	if fileName == "" {
		return domain.PathContentsUploadResult{}, fmt.Errorf("file_name is required")
	}
	if options.Content == nil {
		return domain.PathContentsUploadResult{}, fmt.Errorf("content is required")
	}

	objectPath, err := resolveChildPath(absolutePath, fileName)
	if err != nil {
		return domain.PathContentsUploadResult{}, err
	}

	slog.Debug("catalog UploadPathContents start", "path", absolutePath, "object_path", objectPath, "overwrite", options.Overwrite, "checksum", options.Checksum, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))

	filesystem, err := s.filesystemForRequest(requestContext, "irods-go-rest-upload-path-contents")
	if err != nil {
		logIRODSError("catalog UploadPathContents filesystem setup failed", err, "path", absolutePath, "object_path", objectPath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.PathContentsUploadResult{}, err
	}
	defer filesystem.Release()

	parentEntry, err := filesystem.Stat(absolutePath)
	if err != nil {
		logIRODSError("catalog UploadPathContents parent stat failed", err, "path", absolutePath, "object_path", objectPath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.PathContentsUploadResult{}, normalizePathAccessError("stat path", absolutePath, err)
	}
	if !parentEntry.IsDir() {
		return domain.PathContentsUploadResult{}, fmt.Errorf("%w: path %q is not a collection", ErrNotFound, absolutePath)
	}

	action := "created"
	if existingEntry, statErr := filesystem.Stat(objectPath); statErr == nil {
		if existingEntry.IsDir() {
			return domain.PathContentsUploadResult{}, fmt.Errorf("%w: target path %q is an existing collection", ErrConflict, objectPath)
		}
		if !options.Overwrite {
			return domain.PathContentsUploadResult{}, fmt.Errorf("%w: target data object %q already exists; pass overwrite=true to replace it", ErrConflict, objectPath)
		}
		if err := filesystem.RemoveFile(objectPath, true); err != nil {
			logIRODSError("catalog UploadPathContents remove existing file failed", err, "path", absolutePath, "object_path", objectPath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
			return domain.PathContentsUploadResult{}, normalizePathAccessError("delete data object", objectPath, err)
		}
		if _, _, err := s.waitForObservedPath(ctx, requestContext, objectPath, false, "irods-go-rest-upload-path-contents-overwrite-verify"); err != nil {
			logIRODSError("catalog UploadPathContents overwrite verification failed", err, "path", absolutePath, "object_path", objectPath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
			return domain.PathContentsUploadResult{}, err
		}
		action = "replaced"
	} else if normalizedErr := normalizePathAccessError("stat path", objectPath, statErr); !errors.Is(normalizedErr, ErrNotFound) {
		logIRODSError("catalog UploadPathContents existing stat failed", statErr, "path", absolutePath, "object_path", objectPath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.PathContentsUploadResult{}, normalizedErr
	}

	handle, err := filesystem.CreateFile(objectPath, "", string(irodstypes.FileOpenModeWriteOnly))
	if err != nil {
		logIRODSError("catalog UploadPathContents create file failed", err, "path", absolutePath, "object_path", objectPath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.PathContentsUploadResult{}, normalizePathAccessError("create data object", objectPath, err)
	}

	bytesWritten, copyErr := io.Copy(handle, options.Content)
	closeErr := handle.Close()
	if copyErr != nil {
		_ = filesystem.RemoveFile(objectPath, true)
		logIRODSError("catalog UploadPathContents write failed", copyErr, "path", absolutePath, "object_path", objectPath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.PathContentsUploadResult{}, fmt.Errorf("write uploaded content %q: %w", objectPath, copyErr)
	}
	if closeErr != nil {
		logIRODSError("catalog UploadPathContents close file failed", closeErr, "path", absolutePath, "object_path", objectPath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.PathContentsUploadResult{}, normalizePathAccessError("close data object", objectPath, closeErr)
	}

	verifyFS, _, err := s.waitForObservedPath(ctx, requestContext, objectPath, true, "irods-go-rest-upload-path-contents-verify")
	if err != nil {
		logIRODSError("catalog UploadPathContents verification failed", err, "path", absolutePath, "object_path", objectPath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.PathContentsUploadResult{}, err
	}
	defer verifyFS.Release()

	var checksumInfo *domain.UploadChecksumInfo
	if options.Checksum {
		checksum, err := verifyFS.ComputeChecksum(objectPath, "")
		if err != nil {
			logIRODSError("catalog UploadPathContents checksum failed", err, "path", absolutePath, "object_path", objectPath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
			return domain.PathContentsUploadResult{}, normalizePathAccessError("compute checksum", objectPath, err)
		}

		mappedChecksum := pathChecksumFromIRODSChecksum(checksum)
		checksumInfo = &domain.UploadChecksumInfo{
			Requested: true,
			Verified:  true,
			Algorithm: mappedChecksum.Type,
			Value:     mappedChecksum.Checksum,
		}
	} else {
		checksumInfo = &domain.UploadChecksumInfo{
			Requested: false,
			Verified:  false,
		}
	}

	return domain.PathContentsUploadResult{
		Path:       objectPath,
		ParentPath: absolutePath,
		FileName:   path.Base(objectPath),
		Action:     action,
		Size:       bytesWritten,
		Checksum:   checksumInfo,
	}, nil
}

func (s *catalogService) DeletePath(ctx context.Context, requestContext *RequestContext, absolutePath string, force bool) error {
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
		_, _, err := s.waitForObservedPath(ctx, requestContext, absolutePath, false, "irods-go-rest-delete-path-verify")
		if err != nil {
			logIRODSError("catalog DeletePath verification failed", err, "path", absolutePath, "force", force, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
			return err
		}
		return nil
	}

	if err := filesystem.RemoveFile(absolutePath, force); err != nil {
		logIRODSError("catalog DeletePath remove file failed", err, "path", absolutePath, "force", force, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return normalizePathAccessError("delete data object", absolutePath, err)
	}
	_, _, err = s.waitForObservedPath(ctx, requestContext, absolutePath, false, "irods-go-rest-delete-path-verify")
	if err != nil {
		logIRODSError("catalog DeletePath verification failed", err, "path", absolutePath, "force", force, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return err
	}
	return nil
}

func (s *catalogService) RenamePath(ctx context.Context, requestContext *RequestContext, absolutePath string, newName string) (domain.PathEntry, error) {
	return s.RelocatePath(ctx, requestContext, absolutePath, PathRelocateOptions{
		Operation: PathRelocateOperationMove,
		NewName:   newName,
	})
}

func (s *catalogService) RelocatePath(ctx context.Context, requestContext *RequestContext, absolutePath string, options PathRelocateOptions) (domain.PathEntry, error) {
	absolutePath = strings.TrimSpace(absolutePath)
	if absolutePath == "" {
		return domain.PathEntry{}, fmt.Errorf("%w: path %q", ErrNotFound, absolutePath)
	}

	operation := PathRelocateOperation(strings.ToLower(strings.TrimSpace(string(options.Operation))))
	if operation == "" {
		operation = PathRelocateOperationMove
	}
	if operation != PathRelocateOperationMove && operation != PathRelocateOperationCopy {
		return domain.PathEntry{}, fmt.Errorf("operation must be one of move or copy")
	}

	destPath, err := resolveRelocateDestination(absolutePath, options.NewName, options.DestinationPath)
	if err != nil {
		return domain.PathEntry{}, err
	}
	if path.Clean(destPath) == path.Clean(absolutePath) {
		return domain.PathEntry{}, fmt.Errorf("destination_path must differ from source path")
	}

	applicationName := "irods-go-rest-move-path"
	if operation == PathRelocateOperationCopy {
		applicationName = "irods-go-rest-copy-path"
	}

	slog.Debug("catalog RelocatePath start", "path", absolutePath, "dest_path", destPath, "operation", operation, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))

	filesystem, err := s.filesystemForRequest(requestContext, applicationName)
	if err != nil {
		logIRODSError("catalog RelocatePath filesystem setup failed", err, "path", absolutePath, "dest_path", destPath, "operation", operation, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.PathEntry{}, err
	}
	defer filesystem.Release()

	entry, err := filesystem.Stat(absolutePath)
	if err != nil {
		logIRODSError("catalog RelocatePath stat failed", err, "path", absolutePath, "dest_path", destPath, "operation", operation, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.PathEntry{}, normalizePathAccessError("stat path", absolutePath, err)
	}

	if operation == PathRelocateOperationMove {
		if entry.IsDir() {
			if err := filesystem.RenameDir(absolutePath, destPath); err != nil {
				logIRODSError("catalog RelocatePath rename dir failed", err, "path", absolutePath, "dest_path", destPath, "operation", operation, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
				return domain.PathEntry{}, normalizePathAccessError("move collection", absolutePath, err)
			}
		} else {
			if err := filesystem.RenameFile(absolutePath, destPath); err != nil {
				logIRODSError("catalog RelocatePath rename file failed", err, "path", absolutePath, "dest_path", destPath, "operation", operation, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
				return domain.PathEntry{}, normalizePathAccessError("move data object", absolutePath, err)
			}
		}
	} else {
		if entry.IsDir() {
			if err := copyCollectionRecursive(filesystem, absolutePath, destPath); err != nil {
				logIRODSError("catalog RelocatePath copy dir failed", err, "path", absolutePath, "dest_path", destPath, "operation", operation, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
				return domain.PathEntry{}, normalizePathAccessError("copy collection", absolutePath, err)
			}
		} else {
			if err := filesystem.CopyFile(absolutePath, destPath, false); err != nil {
				logIRODSError("catalog RelocatePath copy file failed", err, "path", absolutePath, "dest_path", destPath, "operation", operation, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
				return domain.PathEntry{}, normalizePathAccessError("copy data object", absolutePath, err)
			}
		}
	}

	verifyFS, relocatedEntry, err := s.waitForObservedPath(ctx, requestContext, destPath, true, applicationName+"-verify")
	if err != nil {
		logIRODSError("catalog RelocatePath destination verification failed", err, "path", absolutePath, "dest_path", destPath, "operation", operation, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.PathEntry{}, err
	}
	defer verifyFS.Release()

	if operation == PathRelocateOperationMove {
		if _, _, err := s.waitForObservedPath(ctx, requestContext, absolutePath, false, applicationName+"-source-verify"); err != nil {
			logIRODSError("catalog RelocatePath source verification failed", err, "path", absolutePath, "dest_path", destPath, "operation", operation, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
			return domain.PathEntry{}, err
		}
	}

	metadata, err := verifyFS.ListMetadata(destPath)
	if err != nil {
		logIRODSError("catalog RelocatePath metadata failed", err, "path", absolutePath, "dest_path", destPath, "operation", operation, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.PathEntry{}, normalizePathAccessError("list metadata", destPath, err)
	}

	if relocatedEntry.IsDir() {
		children, err := verifyFS.List(destPath)
		if err != nil {
			logIRODSError("catalog RelocatePath list children failed", err, "path", absolutePath, "dest_path", destPath, "operation", operation, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
			return domain.PathEntry{}, normalizePathAccessError("list children", destPath, err)
		}
		return collectionPathEntry(s.cfg.IrodsZone, relocatedEntry, metadata, len(children), PathLookupOptions{}), nil
	}

	return dataObjectPathEntry(s.cfg.IrodsZone, relocatedEntry, metadata, PathLookupOptions{}), nil
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

func (s *catalogService) GetPathACL(_ context.Context, requestContext *RequestContext, absolutePath string) (domain.PathACL, error) {
	absolutePath = strings.TrimSpace(absolutePath)
	if absolutePath == "" {
		return domain.PathACL{}, fmt.Errorf("%w: path %q", ErrNotFound, absolutePath)
	}

	slog.Debug("catalog GetPathACL start", "path", absolutePath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))

	filesystem, err := s.filesystemForRequest(requestContext, "irods-go-rest-get-path-acl")
	if err != nil {
		logIRODSError("catalog GetPathACL filesystem setup failed", err, "path", absolutePath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.PathACL{}, err
	}
	defer filesystem.Release()

	entry, err := filesystem.Stat(absolutePath)
	if err != nil {
		logIRODSError("catalog GetPathACL stat failed", err, "path", absolutePath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.PathACL{}, normalizePathAccessError("stat path", absolutePath, err)
	}

	accesses, err := filesystem.ListACLs(absolutePath)
	if err != nil {
		logIRODSError("catalog GetPathACL list ACLs failed", err, "path", absolutePath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.PathACL{}, normalizePathAccessError("list ACLs", absolutePath, err)
	}

	acl := pathACLFromAccesses(absolutePath, pathKindFromEntry(entry), accesses)
	if entry.IsDir() {
		inheritance, err := filesystem.GetDirACLInheritance(absolutePath)
		if err != nil {
			logIRODSError("catalog GetPathACL get inheritance failed", err, "path", absolutePath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
			return domain.PathACL{}, normalizePathAccessError("get ACL inheritance", absolutePath, err)
		}
		if inheritance != nil {
			acl.InheritanceEnabled = boolPointer(inheritance.Inheritance)
		}
	}

	return acl, nil
}

func (s *catalogService) AddPathACL(_ context.Context, requestContext *RequestContext, absolutePath string, acl irodstypes.IRODSAccess, recursive bool) (domain.PathACLEntry, error) {
	absolutePath = strings.TrimSpace(absolutePath)
	if absolutePath == "" {
		return domain.PathACLEntry{}, fmt.Errorf("%w: path %q", ErrNotFound, absolutePath)
	}

	userName := strings.TrimSpace(acl.UserName)
	userZone := strings.TrimSpace(acl.UserZone)
	if userZone == "" {
		userZone = strings.TrimSpace(s.cfg.IrodsZone)
	}
	if userName == "" {
		return domain.PathACLEntry{}, fmt.Errorf("name is required")
	}

	accessLevel := normalizedACLAccessLevel(string(acl.AccessLevel))
	if accessLevel == irodstypes.IRODSAccessLevelNull {
		return domain.PathACLEntry{}, fmt.Errorf("access_level is required")
	}

	slog.Debug("catalog AddPathACL start", "path", absolutePath, "name", userName, "zone", userZone, "access_level", accessLevel, "recursive", recursive, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))

	filesystem, err := s.filesystemForRequest(requestContext, "irods-go-rest-add-path-acl")
	if err != nil {
		logIRODSError("catalog AddPathACL filesystem setup failed", err, "path", absolutePath, "name", userName, "zone", userZone, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.PathACLEntry{}, err
	}
	defer filesystem.Release()

	if _, err := filesystem.Stat(absolutePath); err != nil {
		logIRODSError("catalog AddPathACL stat failed", err, "path", absolutePath, "name", userName, "zone", userZone, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.PathACLEntry{}, normalizePathAccessError("stat path", absolutePath, err)
	}

	if err := filesystem.ChangeACLs(absolutePath, accessLevel, userName, userZone, recursive, false); err != nil {
		logIRODSError("catalog AddPathACL change ACL failed", err, "path", absolutePath, "name", userName, "zone", userZone, "access_level", accessLevel, "recursive", recursive, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.PathACLEntry{}, normalizePathAccessError("change ACL", absolutePath, err)
	}

	accesses, err := filesystem.ListACLs(absolutePath)
	if err != nil {
		logIRODSError("catalog AddPathACL list ACLs failed", err, "path", absolutePath, "name", userName, "zone", userZone, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.PathACLEntry{}, normalizePathAccessError("list ACLs", absolutePath, err)
	}

	for _, access := range accesses {
		if access == nil {
			continue
		}
		if strings.TrimSpace(access.UserName) != userName || strings.TrimSpace(access.UserZone) != userZone {
			continue
		}

		entry := pathACLEntry(access)
		if entry.ID != "" {
			return entry, nil
		}
	}

	return domain.PathACLEntry{
		ID:            aclID("user", userZone, userName),
		Name:          userName,
		Zone:          userZone,
		Type:          "user",
		IRODSUserType: string(irodstypes.IRODSUserRodsUser),
		AccessLevel:   string(accessLevel),
	}, nil
}

func (s *catalogService) UpdatePathACL(_ context.Context, requestContext *RequestContext, absolutePath string, aclEntryID string, accessLevel string, recursive bool) (domain.PathACLEntry, error) {
	absolutePath = strings.TrimSpace(absolutePath)
	if absolutePath == "" {
		return domain.PathACLEntry{}, fmt.Errorf("%w: path %q", ErrNotFound, absolutePath)
	}

	principal, err := parseACLPrincipal(aclEntryID)
	if err != nil {
		return domain.PathACLEntry{}, err
	}
	if principal.Zone == "" {
		principal.Zone = strings.TrimSpace(s.cfg.IrodsZone)
	}

	normalizedAccess := normalizedACLAccessLevel(accessLevel)
	if normalizedAccess == irodstypes.IRODSAccessLevelNull {
		return domain.PathACLEntry{}, fmt.Errorf("access_level is required")
	}

	slog.Debug("catalog UpdatePathACL start", "path", absolutePath, "acl_id", aclEntryID, "access_level", normalizedAccess, "recursive", recursive, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))

	filesystem, err := s.filesystemForRequest(requestContext, "irods-go-rest-update-path-acl")
	if err != nil {
		logIRODSError("catalog UpdatePathACL filesystem setup failed", err, "path", absolutePath, "acl_id", aclEntryID, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.PathACLEntry{}, err
	}
	defer filesystem.Release()

	if _, err := filesystem.Stat(absolutePath); err != nil {
		logIRODSError("catalog UpdatePathACL stat failed", err, "path", absolutePath, "acl_id", aclEntryID, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.PathACLEntry{}, normalizePathAccessError("stat path", absolutePath, err)
	}

	if err := filesystem.ChangeACLs(absolutePath, normalizedAccess, principal.Name, principal.Zone, recursive, false); err != nil {
		logIRODSError("catalog UpdatePathACL change ACL failed", err, "path", absolutePath, "acl_id", aclEntryID, "access_level", normalizedAccess, "recursive", recursive, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.PathACLEntry{}, normalizePathAccessError("change ACL", absolutePath, err)
	}

	accesses, err := filesystem.ListACLs(absolutePath)
	if err != nil {
		logIRODSError("catalog UpdatePathACL list ACLs failed", err, "path", absolutePath, "acl_id", aclEntryID, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.PathACLEntry{}, normalizePathAccessError("list ACLs", absolutePath, err)
	}

	targetID := aclID(principal.Type, principal.Zone, principal.Name)
	for _, access := range accesses {
		entry := pathACLEntry(access)
		if entry.ID == targetID {
			return entry, nil
		}
	}

	return domain.PathACLEntry{
		ID:          targetID,
		Name:        principal.Name,
		Zone:        principal.Zone,
		Type:        principal.Type,
		AccessLevel: string(normalizedAccess),
	}, nil
}

func (s *catalogService) DeletePathACL(_ context.Context, requestContext *RequestContext, absolutePath string, aclEntryID string) error {
	absolutePath = strings.TrimSpace(absolutePath)
	if absolutePath == "" {
		return fmt.Errorf("%w: path %q", ErrNotFound, absolutePath)
	}

	principal, err := parseACLPrincipal(aclEntryID)
	if err != nil {
		return err
	}
	if principal.Zone == "" {
		principal.Zone = strings.TrimSpace(s.cfg.IrodsZone)
	}

	slog.Debug("catalog DeletePathACL start", "path", absolutePath, "acl_id", aclEntryID, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))

	filesystem, err := s.filesystemForRequest(requestContext, "irods-go-rest-delete-path-acl")
	if err != nil {
		logIRODSError("catalog DeletePathACL filesystem setup failed", err, "path", absolutePath, "acl_id", aclEntryID, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return err
	}
	defer filesystem.Release()

	if _, err := filesystem.Stat(absolutePath); err != nil {
		logIRODSError("catalog DeletePathACL stat failed", err, "path", absolutePath, "acl_id", aclEntryID, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return normalizePathAccessError("stat path", absolutePath, err)
	}

	if err := filesystem.ChangeACLs(absolutePath, irodstypes.IRODSAccessLevelNull, principal.Name, principal.Zone, false, false); err != nil {
		logIRODSError("catalog DeletePathACL change ACL failed", err, "path", absolutePath, "acl_id", aclEntryID, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return normalizePathAccessError("change ACL", absolutePath, err)
	}

	return nil
}

func (s *catalogService) SetPathACLInheritance(_ context.Context, requestContext *RequestContext, absolutePath string, enabled bool, recursive bool) error {
	absolutePath = strings.TrimSpace(absolutePath)
	if absolutePath == "" {
		return fmt.Errorf("%w: path %q", ErrNotFound, absolutePath)
	}

	slog.Debug("catalog SetPathACLInheritance start", "path", absolutePath, "enabled", enabled, "recursive", recursive, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))

	filesystem, err := s.filesystemForRequest(requestContext, "irods-go-rest-set-path-acl-inheritance")
	if err != nil {
		logIRODSError("catalog SetPathACLInheritance filesystem setup failed", err, "path", absolutePath, "enabled", enabled, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return err
	}
	defer filesystem.Release()

	entry, err := filesystem.Stat(absolutePath)
	if err != nil {
		logIRODSError("catalog SetPathACLInheritance stat failed", err, "path", absolutePath, "enabled", enabled, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return normalizePathAccessError("stat path", absolutePath, err)
	}
	if !entry.IsDir() {
		return fmt.Errorf("%w: path %q is not a collection", ErrNotFound, absolutePath)
	}

	if err := filesystem.ChangeDirACLInheritance(absolutePath, enabled, recursive, false); err != nil {
		logIRODSError("catalog SetPathACLInheritance change failed", err, "path", absolutePath, "enabled", enabled, "recursive", recursive, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return normalizePathAccessError("change ACL inheritance", absolutePath, err)
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

func (s *catalogService) ListFavorites(_ context.Context, requestContext *RequestContext) ([]domain.Favorite, error) {
	slog.Debug("catalog ListFavorites start", "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))

	filesystem, favoritesPath, err := s.prepareFavoritesFilesystem(requestContext, "irods-go-rest-list-favorites")
	if err != nil {
		return nil, err
	}
	defer filesystem.Release()

	metadata, err := filesystem.ListMetadata(favoritesPath)
	if err != nil {
		logIRODSError("catalog ListFavorites list metadata failed", err, "favorites_path", favoritesPath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return nil, normalizePathAccessError("list metadata", favoritesPath, err)
	}

	favoritesByPath := map[string]domain.Favorite{}
	for _, avu := range metadata {
		favorite, ok := decodeFavoriteFromMetadata(avu)
		if !ok {
			if isFavoriteMetadata(avu) {
				slog.Warn(
					"catalog ListFavorites skipping invalid favorite metadata entry",
					"favorites_path", favoritesPath,
					"avu_id", avu.AVUID,
					"avu_value", strings.TrimSpace(avu.Value),
					"auth_scheme", safeAuthScheme(requestContext),
					"username", safeUsername(requestContext),
				)
			}
			continue
		}
		favoritesByPath[favorite.AbsolutePath] = favorite
	}

	favorites := make([]domain.Favorite, 0, len(favoritesByPath))
	for _, favorite := range favoritesByPath {
		favorites = append(favorites, favorite)
	}

	sort.Slice(favorites, func(i, j int) bool {
		if favorites[i].Name == favorites[j].Name {
			return favorites[i].AbsolutePath < favorites[j].AbsolutePath
		}
		return favorites[i].Name < favorites[j].Name
	})

	return favorites, nil
}

func (s *catalogService) AddFavorite(_ context.Context, requestContext *RequestContext, name string, favoritePath string) (domain.Favorite, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return domain.Favorite{}, fmt.Errorf("favorite name is required")
	}

	normalizedPath, err := normalizeFavoritePath(favoritePath)
	if err != nil {
		return domain.Favorite{}, err
	}

	slog.Debug("catalog AddFavorite start", "favorite_name", name, "favorite_path", normalizedPath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))

	filesystem, favoritesPath, err := s.prepareFavoritesFilesystem(requestContext, "irods-go-rest-add-favorite")
	if err != nil {
		return domain.Favorite{}, err
	}
	defer filesystem.Release()

	metadata, err := filesystem.ListMetadata(favoritesPath)
	if err != nil {
		logIRODSError("catalog AddFavorite list metadata failed", err, "favorites_path", favoritesPath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.Favorite{}, normalizePathAccessError("list metadata", favoritesPath, err)
	}

	encodedValue, err := encodeFavoriteValue(name, normalizedPath)
	if err != nil {
		return domain.Favorite{}, err
	}

	matching := make([]*irodstypes.IRODSMeta, 0)
	hasCanonical := false
	for _, avu := range metadata {
		favorite, ok := decodeFavoriteFromMetadata(avu)
		if !ok || favorite.AbsolutePath != normalizedPath {
			continue
		}

		matching = append(matching, avu)
		if strings.TrimSpace(avu.Value) == encodedValue {
			hasCanonical = true
		}
	}

	if len(matching) == 1 && hasCanonical {
		return domain.Favorite{Name: name, AbsolutePath: normalizedPath}, nil
	}

	for _, avu := range matching {
		if avu == nil {
			continue
		}
		if err := filesystem.DeleteMetadata(favoritesPath, avu.AVUID); err != nil {
			logIRODSError("catalog AddFavorite delete stale favorite metadata failed", err, "favorites_path", favoritesPath, "favorite_path", normalizedPath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
			return domain.Favorite{}, normalizePathAccessError("delete favorite metadata", favoritesPath, err)
		}
	}

	if err := filesystem.AddMetadata(favoritesPath, favoritesAVUAttribute, encodedValue, favoritesAVUUnit); err != nil {
		logIRODSError("catalog AddFavorite add metadata failed", err, "favorites_path", favoritesPath, "favorite_path", normalizedPath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.Favorite{}, normalizePathAccessError("add favorite metadata", favoritesPath, err)
	}

	return domain.Favorite{Name: name, AbsolutePath: normalizedPath}, nil
}

func (s *catalogService) RenameFavorite(_ context.Context, requestContext *RequestContext, favoritePath string, name string) (domain.Favorite, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return domain.Favorite{}, fmt.Errorf("favorite name is required")
	}

	normalizedPath, err := normalizeFavoritePath(favoritePath)
	if err != nil {
		return domain.Favorite{}, err
	}

	slog.Debug("catalog RenameFavorite start", "favorite_name", name, "favorite_path", normalizedPath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))

	filesystem, favoritesPath, err := s.prepareFavoritesFilesystem(requestContext, "irods-go-rest-rename-favorite")
	if err != nil {
		return domain.Favorite{}, err
	}
	defer filesystem.Release()

	metadata, err := filesystem.ListMetadata(favoritesPath)
	if err != nil {
		logIRODSError("catalog RenameFavorite list metadata failed", err, "favorites_path", favoritesPath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.Favorite{}, normalizePathAccessError("list metadata", favoritesPath, err)
	}

	matching := make([]*irodstypes.IRODSMeta, 0)
	for _, avu := range metadata {
		favorite, ok := decodeFavoriteFromMetadata(avu)
		if !ok || favorite.AbsolutePath != normalizedPath {
			continue
		}
		matching = append(matching, avu)
	}

	if len(matching) == 0 {
		return domain.Favorite{}, fmt.Errorf("%w: favorite %q", ErrNotFound, normalizedPath)
	}

	for _, avu := range matching {
		if avu == nil {
			continue
		}
		if err := filesystem.DeleteMetadata(favoritesPath, avu.AVUID); err != nil {
			logIRODSError("catalog RenameFavorite delete existing metadata failed", err, "favorites_path", favoritesPath, "favorite_path", normalizedPath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
			return domain.Favorite{}, normalizePathAccessError("delete favorite metadata", favoritesPath, err)
		}
	}

	encodedValue, err := encodeFavoriteValue(name, normalizedPath)
	if err != nil {
		return domain.Favorite{}, err
	}

	if err := filesystem.AddMetadata(favoritesPath, favoritesAVUAttribute, encodedValue, favoritesAVUUnit); err != nil {
		logIRODSError("catalog RenameFavorite add metadata failed", err, "favorites_path", favoritesPath, "favorite_path", normalizedPath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return domain.Favorite{}, normalizePathAccessError("add favorite metadata", favoritesPath, err)
	}

	return domain.Favorite{Name: name, AbsolutePath: normalizedPath}, nil
}

func (s *catalogService) RemoveFavorite(_ context.Context, requestContext *RequestContext, favoritePath string) error {
	normalizedPath, err := normalizeFavoritePath(favoritePath)
	if err != nil {
		return err
	}

	slog.Debug("catalog RemoveFavorite start", "favorite_path", normalizedPath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))

	filesystem, favoritesPath, err := s.prepareFavoritesFilesystem(requestContext, "irods-go-rest-remove-favorite")
	if err != nil {
		return err
	}
	defer filesystem.Release()

	metadata, err := filesystem.ListMetadata(favoritesPath)
	if err != nil {
		logIRODSError("catalog RemoveFavorite list metadata failed", err, "favorites_path", favoritesPath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
		return normalizePathAccessError("list metadata", favoritesPath, err)
	}

	for _, avu := range metadata {
		favorite, ok := decodeFavoriteFromMetadata(avu)
		if !ok || favorite.AbsolutePath != normalizedPath || avu == nil {
			continue
		}
		if err := filesystem.DeleteMetadata(favoritesPath, avu.AVUID); err != nil {
			logIRODSError("catalog RemoveFavorite delete metadata failed", err, "favorites_path", favoritesPath, "favorite_path", normalizedPath, "auth_scheme", safeAuthScheme(requestContext), "username", safeUsername(requestContext))
			return normalizePathAccessError("delete favorite metadata", favoritesPath, err)
		}
	}

	return nil
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

func (s *catalogService) waitForObservedPath(ctx context.Context, requestContext *RequestContext, irodsPath string, shouldExist bool, applicationName string) (CatalogFileSystem, *irodsfs.Entry, error) {
	const (
		waitTimeout = 3 * time.Second
		waitStep    = 100 * time.Millisecond
	)

	deadline := time.Now().Add(waitTimeout)
	var lastErr error

	for {
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return nil, nil, lastErr
			}
			return nil, nil, ctx.Err()
		default:
		}

		filesystem, err := s.filesystemForRequest(requestContext, applicationName)
		if err != nil {
			return nil, nil, err
		}

		entry, statErr := filesystem.Stat(irodsPath)
		if shouldExist {
			if statErr == nil {
				return filesystem, entry, nil
			}

			filesystem.Release()
			lastErr = normalizePathAccessError("stat path", irodsPath, statErr)
			if !errors.Is(lastErr, ErrNotFound) {
				return nil, nil, lastErr
			}
		} else {
			filesystem.Release()
			if statErr != nil {
				lastErr = normalizePathAccessError("stat path", irodsPath, statErr)
				if errors.Is(lastErr, ErrNotFound) {
					return nil, nil, nil
				}
				return nil, nil, lastErr
			}

			lastErr = fmt.Errorf("path %q is still visible after mutation", irodsPath)
		}

		if time.Now().After(deadline) {
			if shouldExist && lastErr == nil {
				lastErr = fmt.Errorf("%w: path %q", ErrNotFound, irodsPath)
			}
			if !shouldExist && lastErr == nil {
				lastErr = fmt.Errorf("timed out waiting for path %q to disappear", irodsPath)
			}
			return nil, nil, lastErr
		}

		time.Sleep(waitStep)
	}
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
			s.cfg.RequestAuthScheme(),
			requestContext.BasicPassword,
			s.cfg.IrodsDefaultResource,
		)
		if err != nil {
			logIRODSError("catalog direct account creation failed", err, "http_auth_scheme", requestContext.AuthScheme, "irods_proxy_user", requestContext.Username, "irods_client_user", requestContext.Username, "irods_zone", s.cfg.IrodsZone)
			return nil, fmt.Errorf("create iRODS account: %w", err)
		}
		return s.cfg.ApplyIRODSConnectionConfig(account), nil
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
			s.cfg.AdminAuthScheme(),
			s.cfg.IrodsAdminPassword,
			requestContext.Ticket,
			s.cfg.IrodsDefaultResource,
		)
		if err != nil {
			logIRODSError("catalog ticket account creation failed", err, "http_auth_scheme", requestContext.AuthScheme, "irods_proxy_user", s.cfg.IrodsAdminUser, "irods_client_user", s.cfg.IrodsAdminUser, "irods_zone", s.cfg.IrodsZone)
			return nil, fmt.Errorf("create iRODS ticket account: %w", err)
		}
		return s.cfg.ApplyIRODSConnectionConfig(account), nil
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
			s.cfg.AdminAuthScheme(),
			s.cfg.IrodsAdminPassword,
			s.cfg.IrodsDefaultResource,
		)
		if err != nil {
			logIRODSError("catalog proxy account creation failed", err, "http_auth_scheme", requestContext.AuthScheme, "irods_proxy_user", s.cfg.IrodsAdminUser, "irods_client_user", requestContext.Username, "irods_zone", s.cfg.IrodsZone)
			return nil, fmt.Errorf("create iRODS proxy account: %w", err)
		}
		return s.cfg.ApplyIRODSConnectionConfig(account), nil
	default:
		logIRODSError("catalog unsupported auth scheme", fmt.Errorf("unsupported auth scheme %q", requestContext.AuthScheme), "http_auth_scheme", requestContext.AuthScheme, "username", requestContext.Username)
		return nil, fmt.Errorf("unsupported auth scheme %q", requestContext.AuthScheme)
	}
}

func (s *catalogService) requestUserHomePath(requestContext *RequestContext) (string, error) {
	username := strings.TrimSpace(safeUsername(requestContext))
	if username == "" {
		return "", fmt.Errorf("username is required for extension operations")
	}

	zone := strings.TrimSpace(s.cfg.IrodsZone)
	if zone == "" {
		return "", fmt.Errorf("iRODS zone is not configured")
	}

	return path.Join("/", zone, "home", username), nil
}

func (s *catalogService) prepareFavoritesFilesystem(requestContext *RequestContext, applicationName string) (CatalogFileSystem, string, error) {
	filesystem, err := s.filesystemForRequest(requestContext, applicationName)
	if err != nil {
		return nil, "", err
	}

	userHomePath, err := s.requestUserHomePath(requestContext)
	if err != nil {
		filesystem.Release()
		return nil, "", err
	}

	favoritesPath, err := ensureFavoritesPath(filesystem, userHomePath)
	if err != nil {
		filesystem.Release()
		return nil, "", err
	}

	return filesystem, favoritesPath, nil
}

func ensureFavoritesPath(filesystem CatalogFileSystem, userHomePath string) (string, error) {
	rootPath := path.Join(userHomePath, ".irodsext")
	favoritesCollectionPath := path.Join(rootPath, "favorites")
	favoritesPath := path.Join(favoritesCollectionPath, "favorites")

	if err := ensureCollectionPath(filesystem, rootPath); err != nil {
		return "", err
	}
	if err := ensureCollectionPath(filesystem, favoritesCollectionPath); err != nil {
		return "", err
	}
	if err := ensureDataObjectPath(filesystem, favoritesPath); err != nil {
		return "", err
	}

	return favoritesPath, nil
}

func ensureCollectionPath(filesystem CatalogFileSystem, collectionPath string) error {
	entry, err := filesystem.Stat(collectionPath)
	if err != nil {
		if !errors.Is(normalizePathAccessError("stat path", collectionPath, err), ErrNotFound) {
			return normalizePathAccessError("stat path", collectionPath, err)
		}
		if err := filesystem.MakeDir(collectionPath, true); err != nil {
			return normalizePathAccessError("create collection", collectionPath, err)
		}
		return nil
	}

	if entry == nil || !entry.IsDir() {
		return fmt.Errorf("%w: path %q already exists", ErrConflict, collectionPath)
	}

	return nil
}

func ensureDataObjectPath(filesystem CatalogFileSystem, dataObjectPath string) error {
	entry, err := filesystem.Stat(dataObjectPath)
	if err == nil {
		if entry != nil && entry.IsDir() {
			return fmt.Errorf("%w: path %q already exists", ErrConflict, dataObjectPath)
		}
		return nil
	}

	if !errors.Is(normalizePathAccessError("stat path", dataObjectPath, err), ErrNotFound) {
		return normalizePathAccessError("stat path", dataObjectPath, err)
	}

	handle, err := filesystem.CreateFile(dataObjectPath, "", "w")
	if err != nil {
		return normalizePathAccessError("create data object", dataObjectPath, err)
	}
	if err := handle.Close(); err != nil {
		return normalizePathAccessError("close data object", dataObjectPath, err)
	}

	return nil
}

func normalizeFavoritePath(favoritePath string) (string, error) {
	favoritePath = strings.TrimSpace(favoritePath)
	if favoritePath == "" || !path.IsAbs(favoritePath) {
		return "", fmt.Errorf("favorite path must be absolute")
	}

	cleaned := path.Clean(favoritePath)
	if cleaned == "." || cleaned == "/" {
		return "", fmt.Errorf("favorite path must be absolute")
	}

	return cleaned, nil
}

func encodeFavoriteValue(name string, favoritePath string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", fmt.Errorf("favorite name is required")
	}

	normalizedPath, err := normalizeFavoritePath(favoritePath)
	if err != nil {
		return "", err
	}

	payload := FavoritePayload{
		Name:         name,
		AbsolutePath: normalizedPath,
	}

	valueBytes, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}

	return string(valueBytes), nil
}

func decodeFavoriteFromMetadata(avu *irodstypes.IRODSMeta) (domain.Favorite, bool) {
	if !isFavoriteMetadata(avu) {
		return domain.Favorite{}, false
	}

	var payload FavoritePayload
	if err := json.Unmarshal([]byte(strings.TrimSpace(avu.Value)), &payload); err != nil {
		return domain.Favorite{}, false
	}

	favoritePath, err := normalizeFavoritePath(payload.AbsolutePath)
	if err != nil {
		return domain.Favorite{}, false
	}

	name := strings.TrimSpace(payload.Name)
	if name == "" {
		name = path.Base(favoritePath)
	}

	return domain.Favorite{
		Name:         name,
		AbsolutePath: favoritePath,
	}, true
}

func isFavoriteMetadata(avu *irodstypes.IRODSMeta) bool {
	if avu == nil {
		return false
	}

	return strings.TrimSpace(avu.Name) == favoritesAVUAttribute && strings.TrimSpace(avu.Units) == favoritesAVUUnit
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

func (a *catalogFileSystemAdapter) CopyFile(srcPath string, destPath string, force bool) error {
	return a.filesystem.CopyFile(srcPath, destPath, force)
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

func (a *catalogFileSystemAdapter) ListACLs(irodsPath string) ([]*irodstypes.IRODSAccess, error) {
	return a.filesystem.ListACLs(irodsPath)
}

func (a *catalogFileSystemAdapter) GetDirACLInheritance(path string) (*irodstypes.IRODSAccessInheritance, error) {
	return a.filesystem.GetDirACLInheritance(path)
}

func (a *catalogFileSystemAdapter) ChangeACLs(path string, access irodstypes.IRODSAccessLevelType, userName string, zoneName string, recurse bool, adminFlag bool) error {
	return a.filesystem.ChangeACLs(path, access, userName, zoneName, recurse, adminFlag)
}

func (a *catalogFileSystemAdapter) ChangeDirACLInheritance(path string, inherit bool, recurse bool, adminFlag bool) error {
	return a.filesystem.ChangeDirACLInheritance(path, inherit, recurse, adminFlag)
}

func (a *catalogFileSystemAdapter) ComputeChecksum(irodsPath string, resource string) (*irodstypes.IRODSChecksum, error) {
	conn, err := a.filesystem.GetMetadataConnection(false)
	if err != nil {
		return nil, err
	}
	defer a.filesystem.ReturnMetadataConnection(conn) //nolint:errcheck

	return irodslibfs.GetDataObjectChecksum(conn, irodsPath, resource)
}

func (a *catalogFileSystemAdapter) GetServerVersion() (*irodstypes.IRODSVersion, error) {
	return a.filesystem.GetServerVersion()
}

func (a *catalogFileSystemAdapter) OpenFile(irodsPath string, resource string, mode string) (CatalogFileHandle, error) {
	return a.filesystem.OpenFile(irodsPath, resource, mode)
}

func (a *catalogFileSystemAdapter) ListResources() ([]*irodstypes.IRODSResource, error) {
	conn, err := a.filesystem.GetMetadataConnection(false)
	if err != nil {
		return nil, err
	}
	defer a.filesystem.ReturnMetadataConnection(conn) //nolint:errcheck

	return irodslibfs.ListResources(conn)
}

func (a *catalogFileSystemAdapter) GetResource(resourceName string) (*irodstypes.IRODSResource, error) {
	conn, err := a.filesystem.GetMetadataConnection(false)
	if err != nil {
		return nil, err
	}
	defer a.filesystem.ReturnMetadataConnection(conn) //nolint:errcheck

	return irodslibfs.GetResource(conn, resourceName)
}

func (a *catalogFileSystemAdapter) GetUser(username string, zoneName string, userType irodstypes.IRODSUserType) (*irodstypes.IRODSUser, error) {
	return a.filesystem.GetUser(username, zoneName, userType)
}

func (a *catalogFileSystemAdapter) ListUsers(zoneName string, userType irodstypes.IRODSUserType) ([]*irodstypes.IRODSUser, error) {
	return a.filesystem.ListUsers(zoneName, userType)
}

func (a *catalogFileSystemAdapter) ListGroupMembers(zoneName string, groupName string) ([]*irodstypes.IRODSUser, error) {
	return a.filesystem.ListGroupMembers(zoneName, groupName)
}

func (a *catalogFileSystemAdapter) CreateUser(username string, zoneName string, userType irodstypes.IRODSUserType) (*irodstypes.IRODSUser, error) {
	return a.filesystem.CreateUser(username, zoneName, userType)
}

func (a *catalogFileSystemAdapter) ChangeUserPassword(username string, zoneName string, newPassword string) error {
	return a.filesystem.ChangeUserPassword(username, zoneName, newPassword)
}

func (a *catalogFileSystemAdapter) ChangeUserType(username string, zoneName string, newType irodstypes.IRODSUserType) error {
	return a.filesystem.ChangeUserType(username, zoneName, newType)
}

func (a *catalogFileSystemAdapter) RemoveUser(username string, zoneName string, userType irodstypes.IRODSUserType) error {
	return a.filesystem.RemoveUser(username, zoneName, userType)
}

func (a *catalogFileSystemAdapter) AddGroupMember(groupName string, username string, zoneName string) error {
	return a.filesystem.AddGroupMember(groupName, username, zoneName)
}

func (a *catalogFileSystemAdapter) RemoveGroupMember(groupName string, username string, zoneName string) error {
	return a.filesystem.RemoveGroupMember(groupName, username, zoneName)
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

func (a *catalogFileSystemAdapter) ReplicateFile(irodsPath string, resource string, update bool) error {
	return a.filesystem.ReplicateFile(irodsPath, resource, update)
}

func (a *catalogFileSystemAdapter) TrimDataObject(irodsPath string, resource string, minCopies int, minAgeMinutes int) error {
	conn, err := a.filesystem.GetMetadataConnection(false)
	if err != nil {
		return err
	}
	defer a.filesystem.ReturnMetadataConnection(conn) //nolint:errcheck

	if err := irodslibfs.TrimDataObject(conn, irodsPath, resource, minCopies, minAgeMinutes, false); err != nil {
		return err
	}

	// TODO:// evaluate with new nocache irods client
	// Keep immediate Stat() reads consistent after low-level trim operations.
	a.filesystem.InvalidateCacheForFileUpdate(irodsPath)
	return nil
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
	case irodscommon.CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME:
		return fmt.Errorf("%w: path %q already exists", ErrConflict, absolutePath)
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
	if strings.Contains(message, "already has item by that name") || strings.Contains(message, "already exists") {
		return fmt.Errorf("%w: path %q already exists", ErrConflict, absolutePath)
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

func resolveRelocateDestination(sourcePath string, newName string, destinationPath string) (string, error) {
	sourcePath = strings.TrimSpace(sourcePath)
	destinationPath = strings.TrimSpace(destinationPath)
	if sourcePath == "" {
		return "", fmt.Errorf("%w: path %q", ErrNotFound, sourcePath)
	}

	if destinationPath != "" {
		if !path.IsAbs(destinationPath) {
			return "", fmt.Errorf("destination_path must be absolute")
		}

		cleaned := path.Clean(destinationPath)
		if cleaned == "." || cleaned == "" || cleaned == "/" {
			return "", fmt.Errorf("destination_path is invalid")
		}

		return cleaned, nil
	}

	return resolveRenameDestination(sourcePath, newName)
}

func copyCollectionRecursive(filesystem CatalogFileSystem, sourcePath string, destinationPath string) error {
	if _, err := filesystem.Stat(destinationPath); err == nil {
		return fmt.Errorf("%w: path %q already exists", ErrConflict, destinationPath)
	} else if !isPathNotFoundError(err) {
		return err
	}

	if err := filesystem.MakeDir(destinationPath, true); err != nil {
		return err
	}

	entries, err := filesystem.List(sourcePath)
	if err != nil {
		return err
	}

	for _, child := range entries {
		if child == nil {
			continue
		}

		childDestinationPath := path.Join(destinationPath, path.Base(child.Path))
		if child.IsDir() {
			if err := copyCollectionRecursive(filesystem, child.Path, childDestinationPath); err != nil {
				return err
			}
			continue
		}

		if err := filesystem.CopyFile(child.Path, childDestinationPath, false); err != nil {
			return err
		}
	}

	return nil
}

func isPathNotFoundError(err error) bool {
	if err == nil {
		return false
	}

	if irodstypes.IsFileNotFoundError(err) {
		return true
	}

	message := strings.ToLower(err.Error())
	return strings.Contains(message, "not found") || strings.Contains(message, "does not exist")
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

func pathACLFromAccesses(irodsPath string, kind string, accesses []*irodstypes.IRODSAccess) domain.PathACL {
	acl := domain.PathACL{
		IRODSPath: strings.TrimSpace(irodsPath),
		Kind:      kind,
		Users:     []domain.PathACLEntry{},
		Groups:    []domain.PathACLEntry{},
	}

	for _, access := range accesses {
		entry := pathACLEntry(access)
		if entry.ID == "" {
			continue
		}

		if entry.Type == "group" {
			acl.Groups = append(acl.Groups, entry)
			continue
		}

		acl.Users = append(acl.Users, entry)
	}

	sortPathACLEntries(acl.Users)
	sortPathACLEntries(acl.Groups)
	return acl
}

func pathACLEntry(access *irodstypes.IRODSAccess) domain.PathACLEntry {
	if access == nil {
		return domain.PathACLEntry{}
	}

	name := strings.TrimSpace(access.UserName)
	zone := strings.TrimSpace(access.UserZone)
	principalType := principalTypeFromIRODSUserType(access.UserType)
	accessLevel := strings.TrimSpace(string(access.AccessLevel))
	if name == "" || accessLevel == "" {
		return domain.PathACLEntry{}
	}

	return domain.PathACLEntry{
		ID:            aclID(principalType, zone, name),
		Name:          name,
		Zone:          zone,
		Type:          principalType,
		IRODSUserType: strings.TrimSpace(string(access.UserType)),
		AccessLevel:   accessLevel,
	}
}

func principalTypeFromIRODSUserType(userType irodstypes.IRODSUserType) string {
	switch userType {
	case irodstypes.IRODSUserRodsGroup, irodstypes.IRODSUserGroupAdmin:
		return "group"
	default:
		return "user"
	}
}

func aclID(principalType string, zone string, name string) string {
	parts := []string{strings.TrimSpace(principalType), strings.TrimSpace(zone), strings.TrimSpace(name)}
	return strings.Join(parts, ":")
}

type aclPrincipal struct {
	Type string
	Zone string
	Name string
}

func parseACLPrincipal(raw string) (aclPrincipal, error) {
	parts := strings.Split(strings.TrimSpace(raw), ":")
	if len(parts) != 3 {
		return aclPrincipal{}, fmt.Errorf("invalid acl id %q", raw)
	}

	principalType := strings.TrimSpace(parts[0])
	zone := strings.TrimSpace(parts[1])
	name := strings.TrimSpace(parts[2])
	if principalType != "user" && principalType != "group" {
		return aclPrincipal{}, fmt.Errorf("invalid acl id %q", raw)
	}
	if name == "" {
		return aclPrincipal{}, fmt.Errorf("invalid acl id %q", raw)
	}

	return aclPrincipal{
		Type: principalType,
		Zone: zone,
		Name: name,
	}, nil
}

func normalizedACLAccessLevel(raw string) irodstypes.IRODSAccessLevelType {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return irodstypes.IRODSAccessLevelNull
	}

	return irodstypes.GetIRODSAccessLevelType(trimmed)
}

func sortPathACLEntries(entries []domain.PathACLEntry) {
	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].Name != entries[j].Name {
			return entries[i].Name < entries[j].Name
		}
		if entries[i].Zone != entries[j].Zone {
			return entries[i].Zone < entries[j].Zone
		}
		return entries[i].AccessLevel < entries[j].AccessLevel
	})
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

func pathKindFromEntry(entry *irodsfs.Entry) string {
	if entry != nil && entry.IsDir() {
		return "collection"
	}
	return "data_object"
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
	resourceName := firstReplicaResource(entry)
	return domain.PathEntry{
		ID:           entry.Path,
		Path:         entry.Path,
		Kind:         "data_object",
		Checksum:     pathChecksumPointerFromEntry(entry),
		MimeType:     inferredMimeType(entry),
		Size:         entry.Size,
		DisplaySize:  humanReadableSize(entry.Size),
		Zone:         zone,
		Resource:     resourceName,
		ResourceLink: resourceActionLink(resourceName),
		CreatedAt:    timePointer(entry.CreateTime),
		UpdatedAt:    timePointer(entry.ModifyTime),
		Replicas:     pathReplicas(entry, options),
		Metadata:     metadataMap(metadata),
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

func boolPointer(value bool) *bool {
	b := value
	return &b
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
			ResourceLink:      resourceActionLink(replica.ResourceName),
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

func normalizePathChildrenListOptions(options PathChildrenListOptions) PathChildrenListOptions {
	options.NamePattern = strings.TrimSpace(options.NamePattern)
	options.SearchScope = normalizePathChildrenSearchScope(options.SearchScope)
	options.Sort = strings.ToLower(strings.TrimSpace(options.Sort))
	if options.Sort == "" {
		options.Sort = "path"
	}

	options.Order = strings.ToLower(strings.TrimSpace(options.Order))
	if options.Order != "desc" {
		options.Order = "asc"
	}

	if options.Offset < 0 {
		options.Offset = 0
	}
	if options.Limit < 0 {
		options.Limit = 0
	}

	// default case-sensitive matching unless explicitly disabled.
	if !options.CaseSensitive {
		options.CaseSensitive = false
	} else {
		options.CaseSensitive = true
	}

	if options.SearchScope != PathChildrenSearchScopeAbsolute {
		options.NamePattern = strings.TrimLeft(options.NamePattern, "/")
	}
	return options
}

func normalizePathChildrenSearchScope(scope PathChildrenSearchScope) PathChildrenSearchScope {
	switch strings.ToLower(strings.TrimSpace(string(scope))) {
	case string(PathChildrenSearchScopeSubtree):
		return PathChildrenSearchScopeSubtree
	case string(PathChildrenSearchScopeAbsolute):
		return PathChildrenSearchScopeAbsolute
	default:
		return PathChildrenSearchScopeChildren
	}
}

func collectPathChildrenEntries(filesystem CatalogFileSystem, basePath string, recursive bool) ([]*irodsfs.Entry, error) {
	basePath = strings.TrimSpace(basePath)
	if basePath == "" {
		return nil, fmt.Errorf("base path is required")
	}

	children, err := filesystem.List(basePath)
	if err != nil {
		return nil, err
	}
	if !recursive {
		return children, nil
	}

	results := make([]*irodsfs.Entry, 0, len(children))
	queue := make([]string, 0, len(children))
	for _, child := range children {
		if child == nil {
			continue
		}
		results = append(results, child)
		if child.IsDir() {
			queue = append(queue, child.Path)
		}
	}

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		descendants, listErr := filesystem.List(current)
		if listErr != nil {
			return nil, listErr
		}

		sort.SliceStable(descendants, func(i, j int) bool {
			left := descendants[i]
			right := descendants[j]
			if left == nil {
				return false
			}
			if right == nil {
				return true
			}
			return strings.Compare(left.Path, right.Path) < 0
		})

		for _, descendant := range descendants {
			if descendant == nil {
				continue
			}
			results = append(results, descendant)
			if descendant.IsDir() {
				queue = append(queue, descendant.Path)
			}
		}
	}

	return results, nil
}

func filterPathChildrenEntries(entries []*irodsfs.Entry, options PathChildrenListOptions) ([]*irodsfs.Entry, error) {
	if strings.TrimSpace(options.NamePattern) == "" {
		return compactPathChildrenEntries(entries), nil
	}

	filtered := make([]*irodsfs.Entry, 0, len(entries))
	for _, entry := range entries {
		if entry == nil {
			continue
		}

		target := entry.Name
		if options.SearchScope == PathChildrenSearchScopeAbsolute {
			target = entry.Path
		}

		matched, err := wildcardMatch(options.NamePattern, target, options.CaseSensitive)
		if err != nil {
			return nil, fmt.Errorf("invalid name_pattern %q: %w", options.NamePattern, err)
		}
		if matched {
			filtered = append(filtered, entry)
		}
	}

	return filtered, nil
}

func wildcardMatch(pattern string, value string, caseSensitive bool) (bool, error) {
	if !caseSensitive {
		pattern = strings.ToLower(pattern)
		value = strings.ToLower(value)
	}
	return path.Match(pattern, value)
}

func compactPathChildrenEntries(entries []*irodsfs.Entry) []*irodsfs.Entry {
	compacted := make([]*irodsfs.Entry, 0, len(entries))
	for _, entry := range entries {
		if entry != nil {
			compacted = append(compacted, entry)
		}
	}
	return compacted
}

func sortPathChildrenEntries(entries []*irodsfs.Entry, sortField string, order string) {
	desc := strings.EqualFold(order, "desc")

	sort.SliceStable(entries, func(i, j int) bool {
		left := entries[i]
		right := entries[j]
		if left == nil {
			return false
		}
		if right == nil {
			return true
		}

		var cmp int
		switch sortField {
		case "name":
			cmp = strings.Compare(left.Name, right.Name)
		case "kind":
			leftKind := "data_object"
			rightKind := "data_object"
			if left.IsDir() {
				leftKind = "collection"
			}
			if right.IsDir() {
				rightKind = "collection"
			}
			cmp = strings.Compare(leftKind, rightKind)
		case "size":
			if left.Size < right.Size {
				cmp = -1
			} else if left.Size > right.Size {
				cmp = 1
			}
		case "created_at":
			cmp = compareIRODSEntryTime(left.CreateTime, right.CreateTime)
		case "updated_at":
			cmp = compareIRODSEntryTime(left.ModifyTime, right.ModifyTime)
		default:
			cmp = strings.Compare(left.Path, right.Path)
		}

		if cmp == 0 {
			cmp = strings.Compare(left.Path, right.Path)
		}
		if desc {
			return cmp > 0
		}
		return cmp < 0
	})
}

func compareIRODSEntryTime(left time.Time, right time.Time) int {
	switch {
	case left.Before(right):
		return -1
	case left.After(right):
		return 1
	default:
		return 0
	}
}

func paginatePathChildrenEntries(entries []*irodsfs.Entry, offset int, limit int) []*irodsfs.Entry {
	if offset < 0 {
		offset = 0
	}
	if limit < 0 {
		limit = 0
	}

	if offset >= len(entries) {
		return []*irodsfs.Entry{}
	}

	start := offset
	end := len(entries)
	if limit > 0 && start+limit < end {
		end = start + limit
	}
	return entries[start:end]
}

func mapPathChildrenEntries(zone string, entries []*irodsfs.Entry) []domain.PathEntry {
	results := make([]domain.PathEntry, 0, len(entries))
	for _, child := range entries {
		if child == nil {
			continue
		}
		if child.IsDir() {
			results = append(results, collectionPathEntry(zone, child, nil, 0, PathLookupOptions{}))
			continue
		}
		results = append(results, dataObjectPathEntry(zone, child, nil, PathLookupOptions{}))
	}
	return results
}

func entryHasReplicaInResource(entry *irodsfs.Entry, resource string) bool {
	if entry == nil {
		return false
	}

	resource = strings.TrimSpace(resource)
	if resource == "" {
		return false
	}

	for _, replica := range entry.IRODSReplicas {
		if strings.TrimSpace(replica.ResourceName) == resource {
			return true
		}
	}

	return false
}

func resolveReplicaResourceByIndex(entry *irodsfs.Entry, replicaIndex *int64) (string, error) {
	if entry == nil {
		return "", fmt.Errorf("resource or replica_index is required")
	}
	if replicaIndex == nil {
		return "", fmt.Errorf("resource or replica_index is required")
	}

	for _, replica := range entry.IRODSReplicas {
		if replica.Number == *replicaIndex {
			resource := strings.TrimSpace(replica.ResourceName)
			if resource == "" {
				break
			}
			return resource, nil
		}
	}

	return "", fmt.Errorf("%w: replica_index %d on path %q", ErrNotFound, *replicaIndex, entry.Path)
}

func resourceActionLink(resourceName string) *domain.ActionLink {
	resourceName = strings.TrimSpace(resourceName)
	if resourceName == "" {
		return nil
	}

	return &domain.ActionLink{
		Href:   "/api/v1/resource/" + url.PathEscape(resourceName),
		Method: "GET",
	}
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
