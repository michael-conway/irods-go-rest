package irods

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"mime"
	"path/filepath"
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

type RequestContext struct {
	AuthScheme    string
	Username      string
	BasicPassword string
	Ticket        string
}

type PathLookupOptions struct {
	VerboseLevel int
}

type CatalogService interface {
	GetPath(ctx context.Context, requestContext *RequestContext, absolutePath string, options PathLookupOptions) (domain.PathEntry, error)
	GetPathChildren(ctx context.Context, requestContext *RequestContext, absolutePath string) ([]domain.PathEntry, error)
	GetPathMetadata(ctx context.Context, requestContext *RequestContext, absolutePath string) ([]domain.AVUMetadata, error)
	GetPathChecksum(ctx context.Context, requestContext *RequestContext, absolutePath string) (domain.PathChecksum, error)
	ComputePathChecksum(ctx context.Context, requestContext *RequestContext, absolutePath string) (domain.PathChecksum, error)
	GetObjectContentByPath(ctx context.Context, requestContext *RequestContext, absolutePath string) (domain.ObjectContent, error)
}

type CatalogFileSystem interface {
	Stat(irodsPath string) (*irodsfs.Entry, error)
	List(irodsPath string) ([]*irodsfs.Entry, error)
	ListMetadata(irodsPath string) ([]*irodstypes.IRODSMeta, error)
	ComputeChecksum(irodsPath string, resource string) (*irodstypes.IRODSChecksum, error)
	OpenFile(irodsPath string, resource string, mode string) (CatalogFileHandle, error)
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
		ContentType: inferredMimeType(entry),
		Size:        entry.Size,
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

func (a *catalogFileSystemAdapter) ListMetadata(irodsPath string) ([]*irodstypes.IRODSMeta, error) {
	return a.filesystem.ListMetadata(irodsPath)
}

func (a *catalogFileSystemAdapter) ComputeChecksum(irodsPath string, resource string) (*irodstypes.IRODSChecksum, error) {
	conn, err := a.filesystem.GetMetadataConnection()
	if err != nil {
		return nil, err
	}
	defer a.filesystem.ReturnMetadataConnection(conn) //nolint:errcheck

	return irodslibfs.GetDataObjectChecksum(conn, irodsPath, resource)
}

func (a *catalogFileSystemAdapter) OpenFile(irodsPath string, resource string, mode string) (CatalogFileHandle, error) {
	return a.filesystem.OpenFile(irodsPath, resource, mode)
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
		Checksum:    checksumString(entry),
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
