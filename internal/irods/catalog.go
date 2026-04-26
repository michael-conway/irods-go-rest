package irods

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"path/filepath"
	"strings"

	irodsfs "github.com/cyverse/go-irodsclient/fs"
	irodscommon "github.com/cyverse/go-irodsclient/irods/common"
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

type CatalogService interface {
	GetPath(ctx context.Context, requestContext *RequestContext, absolutePath string) (domain.PathEntry, error)
	GetPathChildren(ctx context.Context, requestContext *RequestContext, absolutePath string) ([]domain.PathEntry, error)
	GetObjectContentByPath(ctx context.Context, requestContext *RequestContext, absolutePath string) (domain.ObjectContent, error)
}

type CatalogFileSystem interface {
	Stat(irodsPath string) (*irodsfs.Entry, error)
	List(irodsPath string) ([]*irodsfs.Entry, error)
	ListMetadata(irodsPath string) ([]*irodstypes.IRODSMeta, error)
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

func (s *catalogService) GetPath(_ context.Context, requestContext *RequestContext, absolutePath string) (domain.PathEntry, error) {
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

		return domain.PathEntry{
			ID:          absolutePath,
			Path:        absolutePath,
			Kind:        "collection",
			Zone:        s.cfg.IrodsZone,
			HasChildren: len(children) > 0,
			ChildCount:  len(children),
			Metadata:    metadataMap(metadata),
		}, nil
	}

	return domain.PathEntry{
		ID:       absolutePath,
		Path:     absolutePath,
		Kind:     "data_object",
		Checksum: checksumString(entry),
		Size:     entry.Size,
		Zone:     s.cfg.IrodsZone,
		Resource: firstReplicaResource(entry),
		Metadata: metadataMap(metadata),
	}, nil
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
			results = append(results, domain.PathEntry{
				ID:          child.Path,
				Path:        child.Path,
				Kind:        "collection",
				Zone:        s.cfg.IrodsZone,
				HasChildren: false,
			})
			continue
		}

		results = append(results, domain.PathEntry{
			ID:       child.Path,
			Path:     child.Path,
			Kind:     "data_object",
			Zone:     s.cfg.IrodsZone,
			Size:     child.Size,
			Checksum: checksumString(child),
			Resource: firstReplicaResource(child),
		})
	}

	return results, nil
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

	contentType := mime.TypeByExtension(filepath.Ext(absolutePath))
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	return domain.ObjectContent{
		Path:        absolutePath,
		ContentType: contentType,
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

func firstReplicaResource(entry *irodsfs.Entry) string {
	if entry == nil || len(entry.IRODSReplicas) == 0 || entry.IRODSReplicas[0].ResourceName == "" {
		return ""
	}

	return entry.IRODSReplicas[0].ResourceName
}
