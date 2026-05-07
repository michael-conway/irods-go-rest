package irods

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"sort"
	"strings"

	irodstypes "github.com/cyverse/go-irodsclient/irods/types"
	s3adminext "github.com/michael-conway/go-irodsclient-extensions/s3admin"
	"github.com/michael-conway/irods-go-rest/internal/domain"
	"github.com/rs/xid"
)

type S3BucketListOptions struct {
	IRODSPath  string
	BucketName string
	Recursive  bool
}

type S3BucketUpsertOptions struct {
	BucketName   string
	AutoGenerate bool
}

type S3UserSecretStoreOptions struct {
	SecretKey    string
	AutoGenerate bool
}

func (s *catalogService) ListS3Buckets(_ context.Context, requestContext *RequestContext, options S3BucketListOptions) ([]domain.S3Bucket, error) {
	filesystem, service, err := s.prepareS3AdminService(requestContext, "irods-go-rest-list-s3-buckets")
	if err != nil {
		return nil, err
	}
	defer filesystem.Release()

	buckets, err := service.ListBuckets(s3adminext.ListOptions{
		IRODSPath:  options.IRODSPath,
		BucketName: options.BucketName,
		Recursive:  options.Recursive,
	})
	if err != nil {
		return nil, normalizeS3AdminError("list buckets", "", err)
	}

	return s3BucketsFromExtension(buckets), nil
}

func (s *catalogService) GetS3Bucket(_ context.Context, requestContext *RequestContext, bucketID string, options S3BucketListOptions) (domain.S3Bucket, error) {
	bucketID = strings.TrimSpace(bucketID)
	if bucketID == "" {
		return domain.S3Bucket{}, fmt.Errorf("bucket id is required")
	}

	filesystem, service, err := s.prepareS3AdminService(requestContext, "irods-go-rest-get-s3-bucket")
	if err != nil {
		return domain.S3Bucket{}, err
	}
	defer filesystem.Release()

	buckets, err := service.SearchBuckets(bucketID, s3adminext.ListOptions{
		IRODSPath: options.IRODSPath,
		Recursive: options.Recursive,
	})
	if err != nil {
		return domain.S3Bucket{}, normalizeS3AdminError("search bucket", "", err)
	}
	if len(buckets) == 0 {
		return domain.S3Bucket{}, fmt.Errorf("%w: bucket %q", ErrNotFound, bucketID)
	}
	if len(buckets) > 1 {
		return domain.S3Bucket{}, fmt.Errorf("%w: bucket %q maps to multiple collections", ErrConflict, bucketID)
	}

	return s3BucketFromExtension(buckets[0]), nil
}

func (s *catalogService) GetS3BucketByPath(_ context.Context, requestContext *RequestContext, irodsPath string) (domain.S3Bucket, error) {
	normalizedPath, err := normalizeS3IRODSPath(irodsPath)
	if err != nil {
		return domain.S3Bucket{}, err
	}

	filesystem, service, err := s.prepareS3AdminService(requestContext, "irods-go-rest-get-s3-bucket-by-path")
	if err != nil {
		return domain.S3Bucket{}, err
	}
	defer filesystem.Release()

	buckets, err := service.ListBuckets(s3adminext.ListOptions{
		IRODSPath: normalizedPath,
		Recursive: false,
	})
	if err != nil {
		return domain.S3Bucket{}, normalizeS3AdminError("list buckets by path", normalizedPath, err)
	}

	matching := make([]s3adminext.Bucket, 0, len(buckets))
	for _, bucket := range buckets {
		if path.Clean(bucket.IRODSPath) == normalizedPath {
			matching = append(matching, bucket)
		}
	}
	if len(matching) == 0 {
		return domain.S3Bucket{}, fmt.Errorf("%w: bucket for path %q", ErrNotFound, normalizedPath)
	}
	if len(matching) > 1 {
		return domain.S3Bucket{}, fmt.Errorf("%w: path %q has multiple buckets", ErrConflict, normalizedPath)
	}

	return s3BucketFromExtension(matching[0]), nil
}

func (s *catalogService) UpsertS3Bucket(_ context.Context, requestContext *RequestContext, irodsPath string, options S3BucketUpsertOptions) (domain.S3Bucket, bool, error) {
	normalizedPath, err := normalizeS3IRODSPath(irodsPath)
	if err != nil {
		return domain.S3Bucket{}, false, err
	}

	bucketName := strings.TrimSpace(options.BucketName)
	if bucketName == "" && options.AutoGenerate {
		bucketName = xid.New().String()
	}
	if bucketName == "" {
		return domain.S3Bucket{}, false, fmt.Errorf("bucket_name is required unless auto_generate is true")
	}

	filesystem, service, err := s.prepareS3AdminService(requestContext, "irods-go-rest-upsert-s3-bucket")
	if err != nil {
		return domain.S3Bucket{}, false, err
	}
	defer filesystem.Release()

	existingBuckets, err := service.ListBuckets(s3adminext.ListOptions{
		IRODSPath: normalizedPath,
		Recursive: false,
	})
	if err != nil {
		return domain.S3Bucket{}, false, normalizeS3AdminError("list existing bucket", normalizedPath, err)
	}

	hasExisting := false
	for _, bucket := range existingBuckets {
		if path.Clean(bucket.IRODSPath) == normalizedPath {
			hasExisting = true
			break
		}
	}

	var bucket s3adminext.Bucket
	if hasExisting {
		bucket, err = service.UpdateBucket(normalizedPath, bucketName)
	} else {
		bucket, err = service.AddBucket(normalizedPath, bucketName)
	}
	if err != nil {
		return domain.S3Bucket{}, false, normalizeS3AdminError("upsert bucket", normalizedPath, err)
	}

	return s3BucketFromExtension(bucket), !hasExisting, nil
}

func (s *catalogService) DeleteS3Bucket(ctx context.Context, requestContext *RequestContext, bucketID string) error {
	bucket, err := s.GetS3Bucket(ctx, requestContext, bucketID, S3BucketListOptions{Recursive: true})
	if err != nil {
		return err
	}

	filesystem, service, err := s.prepareS3AdminService(requestContext, "irods-go-rest-delete-s3-bucket")
	if err != nil {
		return err
	}
	defer filesystem.Release()

	if err := service.DeleteBucket(bucket.IRODSPath); err != nil {
		return normalizeS3AdminError("delete bucket", bucket.IRODSPath, err)
	}
	return nil
}

func (s *catalogService) RebuildS3BucketMapping(_ context.Context, requestContext *RequestContext) (domain.S3BucketMappingRefresh, error) {
	filesystem, service, err := s.prepareS3AdminService(requestContext, "irods-go-rest-rebuild-s3-bucket-mapping")
	if err != nil {
		return domain.S3BucketMappingRefresh{}, err
	}
	defer filesystem.Release()

	if err := s.requireS3RodsAdminUser(filesystem, requestContext, "s3 bucket mapping refresh"); err != nil {
		return domain.S3BucketMappingRefresh{}, err
	}

	result, err := service.RebuildMappingFromAVUs()
	if err != nil {
		return domain.S3BucketMappingRefresh{}, normalizeS3AdminError("rebuild s3 bucket mapping", "", err)
	}

	buckets := s3BucketsFromExtension(result.Buckets)
	return domain.S3BucketMappingRefresh{
		MappingFilePath: result.MappingFilePath,
		Buckets:         buckets,
		Count:           len(buckets),
	}, nil
}

func (s *catalogService) ListS3UserSecrets(_ context.Context, requestContext *RequestContext) ([]domain.S3UserSecret, error) {
	filesystem, service, err := s.prepareS3UserMappingService(requestContext, "irods-go-rest-list-s3-user-secrets", false)
	if err != nil {
		return nil, err
	}
	defer filesystem.Release()

	if err := s.requireS3RodsAdminUser(filesystem, requestContext, "s3 user secret list"); err != nil {
		return nil, err
	}

	users, err := service.ListUserSecretMappingsFromAVUs()
	if err != nil {
		return nil, normalizeS3AdminError("list s3 user secrets", "", err)
	}

	return s3UserSecretsFromExtension(users), nil
}

func (s *catalogService) GetS3UserSecret(_ context.Context, requestContext *RequestContext, userName string) (domain.S3UserSecret, error) {
	userAccount, err := s.s3UserAccount(userName)
	if err != nil {
		return domain.S3UserSecret{}, err
	}
	secretPath, _ := s3adminext.S3UserKeyPath(userAccount)

	filesystem, service, err := s.prepareS3UserMappingService(requestContext, "irods-go-rest-get-s3-user-secret", false)
	if err != nil {
		return domain.S3UserSecret{}, err
	}
	defer filesystem.Release()

	userSecret, err := service.GetUserSecretKey(userAccount)
	if err != nil {
		return domain.S3UserSecret{}, normalizeS3AdminError("get s3 user secret", secretPath, err)
	}
	return s3UserSecretFromExtension(userSecret), nil
}

func (s *catalogService) StoreS3UserSecret(_ context.Context, requestContext *RequestContext, userName string, options S3UserSecretStoreOptions) (domain.S3UserSecret, error) {
	userAccount, err := s.s3UserAccount(userName)
	if err != nil {
		return domain.S3UserSecret{}, err
	}
	secretPath, _ := s3adminext.S3UserKeyPath(userAccount)

	filesystem, service, err := s.prepareS3UserMappingService(requestContext, "irods-go-rest-store-s3-user-secret", false)
	if err != nil {
		return domain.S3UserSecret{}, err
	}
	defer filesystem.Release()

	var userSecret s3adminext.S3UserMapping
	if options.AutoGenerate {
		userSecret, err = service.GenerateAndStoreUserSecretKey(userAccount)
	} else {
		userSecret, err = service.StoreUserSecretKey(userAccount, strings.TrimSpace(options.SecretKey))
	}
	if err != nil {
		return domain.S3UserSecret{}, normalizeS3AdminError("store s3 user secret", secretPath, err)
	}
	return s3UserSecretFromExtension(userSecret), nil
}

func (s *catalogService) DeleteS3UserSecret(_ context.Context, requestContext *RequestContext, userName string) error {
	userAccount, err := s.s3UserAccount(userName)
	if err != nil {
		return err
	}

	filesystem, service, err := s.prepareS3UserMappingService(requestContext, "irods-go-rest-delete-s3-user-secret", false)
	if err != nil {
		return err
	}
	defer filesystem.Release()

	if err := service.DeleteUserSecretKey(userAccount); err != nil {
		secretPath, _ := s3adminext.S3UserKeyPath(userAccount)
		return normalizeS3AdminError("delete s3 user secret", secretPath, err)
	}
	return nil
}

func (s *catalogService) RebuildS3UserMapping(_ context.Context, requestContext *RequestContext) (domain.S3UserSecretMappingRefresh, error) {
	filesystem, service, err := s.prepareS3UserMappingService(requestContext, "irods-go-rest-rebuild-s3-user-mapping", true)
	if err != nil {
		return domain.S3UserSecretMappingRefresh{}, err
	}
	defer filesystem.Release()

	if err := s.requireS3RodsAdminUser(filesystem, requestContext, "s3 user mapping refresh"); err != nil {
		return domain.S3UserSecretMappingRefresh{}, err
	}

	result, err := service.RebuildUserMappingFromAVUs()
	if err != nil {
		return domain.S3UserSecretMappingRefresh{}, normalizeS3AdminError("rebuild s3 user mapping", "", err)
	}

	users := s3UserSecretsFromExtension(result.Users)
	return domain.S3UserSecretMappingRefresh{
		MappingFilePath: result.MappingFilePath,
		Users:           users,
		Count:           len(users),
	}, nil
}

func (s *catalogService) prepareS3AdminService(requestContext *RequestContext, applicationName string) (CatalogFileSystem, *s3adminext.S3Service, error) {
	if !s.cfg.S3ApiSupported {
		return nil, nil, ErrS3AdminNotSupported
	}

	mappingPath := strings.TrimSpace(s.cfg.S3BucketMappingFile)
	if mappingPath == "" || s.s3BucketMappingFile == nil {
		return nil, nil, fmt.Errorf("%w: S3BucketMappingFile is required", ErrS3AdminNotConfigured)
	}
	if !filepath.IsAbs(mappingPath) {
		return nil, nil, fmt.Errorf("%w: S3BucketMappingFile must be an absolute path", ErrS3AdminNotConfigured)
	}

	zone := strings.TrimSpace(s.cfg.IrodsZone)
	if zone == "" {
		return nil, nil, fmt.Errorf("%w: IrodsZone is required", ErrS3AdminNotConfigured)
	}
	scanRoot := path.Join("/", zone)

	filesystem, err := s.filesystemForRequest(requestContext, applicationName)
	if err != nil {
		return nil, nil, err
	}

	service, err := s3adminext.NewS3ServiceWithMappingFile(s3AdminFilesystemFor(filesystem), scanRoot, s.s3BucketMappingFile)
	if err != nil {
		filesystem.Release()
		return nil, nil, normalizeS3AdminError("create s3admin service", scanRoot, err)
	}

	return filesystem, service, nil
}

func (s *catalogService) prepareS3UserMappingService(requestContext *RequestContext, applicationName string, useAdminAccount bool) (CatalogFileSystem, *s3adminext.S3UserMappingService, error) {
	if !s.cfg.S3ApiSupported {
		return nil, nil, ErrS3AdminNotSupported
	}

	mappingPath := strings.TrimSpace(s.cfg.S3UserMappingFile)
	if mappingPath == "" || s.s3UserMappingFile == nil {
		return nil, nil, fmt.Errorf("%w: S3UserMappingFile is required", ErrS3AdminNotConfigured)
	}
	if !filepath.IsAbs(mappingPath) {
		return nil, nil, fmt.Errorf("%w: S3UserMappingFile must be an absolute path", ErrS3AdminNotConfigured)
	}

	var filesystem CatalogFileSystem
	var err error
	if useAdminAccount {
		filesystem, err = s.adminFilesystem(applicationName)
	} else {
		filesystem, err = s.filesystemForRequest(requestContext, applicationName)
	}
	if err != nil {
		return nil, nil, err
	}

	service, err := s3adminext.NewS3UserMappingServiceWithMappingFile(s3AdminUserMappingFilesystemFor(filesystem), s.s3UserMappingFile)
	if err != nil {
		filesystem.Release()
		return nil, nil, normalizeS3AdminError("create s3 user mapping service", mappingPath, err)
	}

	return filesystem, service, nil
}

func (s *catalogService) adminFilesystem(applicationName string) (CatalogFileSystem, error) {
	account := s.cfg.ToIrodsAccount()
	filesystem, err := s.createFileSystem(&account, applicationName)
	if err != nil {
		logIRODSError(
			"catalog adminFilesystem connect failed",
			err,
			"application_name", applicationName,
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

func (s *catalogService) requireS3RodsAdminUser(filesystem CatalogFileSystem, requestContext *RequestContext, operation string) error {
	username := strings.TrimSpace(safeUsername(requestContext))
	if username == "" {
		return fmt.Errorf("%w: insufficient privilege: %s requires an iRODS user with rodsadmin type", ErrPermissionDenied, operation)
	}

	zone := strings.TrimSpace(s.cfg.IrodsZone)
	if zone == "" {
		return fmt.Errorf("%w: IrodsZone is required", ErrS3AdminNotConfigured)
	}

	user, err := filesystem.GetUser(username, zone, "")
	if err != nil {
		return fmt.Errorf("%w: insufficient privilege: %s requires an iRODS user with rodsadmin type", ErrPermissionDenied, operation)
	}
	if user == nil || user.Type != irodstypes.IRODSUserRodsAdmin {
		return fmt.Errorf("%w: insufficient privilege: %s requires an iRODS user with rodsadmin type", ErrPermissionDenied, operation)
	}

	return nil
}

func (s *catalogService) s3UserAccount(userName string) (*irodstypes.IRODSAccount, error) {
	userName = strings.TrimSpace(userName)
	if userName == "" {
		return nil, fmt.Errorf("user_name is required")
	}
	if strings.Contains(userName, "/") {
		return nil, fmt.Errorf("user_name must not contain path separators")
	}

	zone := strings.TrimSpace(s.cfg.IrodsZone)
	if zone == "" {
		return nil, fmt.Errorf("%w: IrodsZone is required", ErrS3AdminNotConfigured)
	}

	account := s.cfg.ToIrodsAccount()
	account.ClientUser = userName
	account.ClientZone = zone
	account.ProxyUser = userName
	account.ProxyZone = zone
	return &account, nil
}

func normalizeS3AdminError(operation string, irodsPath string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, s3adminext.ErrDuplicateUserMapping) {
		return fmt.Errorf("%w: %s", ErrConflict, err.Error())
	}
	if errors.Is(err, s3adminext.ErrDuplicateBucket) {
		return fmt.Errorf("%w: %s", ErrConflict, err.Error())
	}
	if errors.Is(err, s3adminext.ErrBucketAlreadySet) {
		return fmt.Errorf("%w: %s", ErrConflict, err.Error())
	}
	if errors.Is(err, s3adminext.ErrBucketNotFound) {
		return fmt.Errorf("%w: %s", ErrNotFound, err.Error())
	}
	if errors.Is(err, s3adminext.ErrUserSecretMarkerNotFound) {
		return fmt.Errorf("%w: %s", ErrNotFound, err.Error())
	}
	if errors.Is(err, s3adminext.ErrInvalidUserSecretKey) ||
		errors.Is(err, s3adminext.ErrInvalidUserID) ||
		errors.Is(err, s3adminext.ErrInvalidUserSecretKeyIRODSPath) {
		return err
	}
	if errors.Is(err, s3adminext.ErrInvalidBucketName) || errors.Is(err, s3adminext.ErrInvalidIRODSPath) || errors.Is(err, s3adminext.ErrInvalidScanRoot) {
		return err
	}
	if irodsPath != "" {
		return normalizePathAccessError(operation, irodsPath, err)
	}
	return err
}

func normalizeS3IRODSPath(irodsPath string) (string, error) {
	irodsPath = strings.TrimSpace(irodsPath)
	if irodsPath == "" {
		return "", fmt.Errorf("irods_path is required")
	}
	if !path.IsAbs(irodsPath) {
		return "", fmt.Errorf("irods_path must be an absolute iRODS path")
	}
	cleaned := path.Clean(irodsPath)
	if cleaned == "." || cleaned == "/" {
		return "", fmt.Errorf("irods_path must be an absolute iRODS collection path")
	}
	return cleaned, nil
}

func s3BucketsFromExtension(buckets []s3adminext.Bucket) []domain.S3Bucket {
	if len(buckets) == 0 {
		return []domain.S3Bucket{}
	}

	mapped := make([]domain.S3Bucket, 0, len(buckets))
	for _, bucket := range buckets {
		mapped = append(mapped, s3BucketFromExtension(bucket))
	}
	sort.Slice(mapped, func(i, j int) bool {
		if mapped[i].BucketID == mapped[j].BucketID {
			return mapped[i].IRODSPath < mapped[j].IRODSPath
		}
		return mapped[i].BucketID < mapped[j].BucketID
	})
	return mapped
}

func s3BucketFromExtension(bucket s3adminext.Bucket) domain.S3Bucket {
	return domain.S3Bucket{
		BucketID:  bucket.Name,
		IRODSPath: bucket.IRODSPath,
	}
}

func s3UserSecretsFromExtension(users []s3adminext.S3UserMapping) []domain.S3UserSecret {
	if len(users) == 0 {
		return []domain.S3UserSecret{}
	}

	mapped := make([]domain.S3UserSecret, 0, len(users))
	for _, user := range users {
		mapped = append(mapped, s3UserSecretFromExtension(user))
	}
	sort.Slice(mapped, func(i, j int) bool {
		return mapped[i].UserName < mapped[j].UserName
	})
	return mapped
}

func s3UserSecretFromExtension(user s3adminext.S3UserMapping) domain.S3UserSecret {
	userName := strings.TrimSpace(user.UserID)
	if userName == "" {
		userName = strings.TrimSpace(user.Username)
	}
	return domain.S3UserSecret{
		UserName:     userName,
		UserHomePath: user.UserHomePath,
		IRODSPath:    user.IRODSPath,
		SecretKey:    user.SecretKey,
	}
}

type s3AdminFilesystemAdapter struct {
	filesystem CatalogFileSystem
}

type s3AdminFilesystemProvider interface {
	S3AdminFilesystem() s3adminext.Filesystem
}

type s3AdminUserMappingFilesystemProvider interface {
	S3AdminUserMappingFilesystem() s3adminext.UserMappingFilesystem
}

func s3AdminFilesystemFor(filesystem CatalogFileSystem) s3adminext.Filesystem {
	if provider, ok := filesystem.(s3AdminFilesystemProvider); ok {
		if s3Filesystem := provider.S3AdminFilesystem(); s3Filesystem != nil {
			return s3Filesystem
		}
	}
	return &s3AdminFilesystemAdapter{filesystem: filesystem}
}

func s3AdminUserMappingFilesystemFor(filesystem CatalogFileSystem) s3adminext.UserMappingFilesystem {
	if provider, ok := filesystem.(s3AdminUserMappingFilesystemProvider); ok {
		if s3Filesystem := provider.S3AdminUserMappingFilesystem(); s3Filesystem != nil {
			return s3Filesystem
		}
	}
	return &s3AdminFilesystemAdapter{filesystem: filesystem}
}

func (adapter *s3AdminFilesystemAdapter) CollectionExists(irodsPath string) (bool, error) {
	entry, err := adapter.filesystem.Stat(irodsPath)
	if err != nil {
		if errors.Is(normalizePathAccessError("stat path", irodsPath, err), ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return entry != nil && entry.IsDir(), nil
}

func (adapter *s3AdminFilesystemAdapter) CreateCollection(irodsPath string, recurse bool) error {
	return adapter.filesystem.MakeDir(irodsPath, recurse)
}

func (adapter *s3AdminFilesystemAdapter) SearchByMeta(metaName string, metaValue string) ([]s3adminext.Entry, error) {
	return adapter.filesystem.SearchByMeta(metaName, metaValue)
}

func (adapter *s3AdminFilesystemAdapter) ListCollectionMetadata(collectionPath string) ([]s3adminext.Metadata, error) {
	metadata, err := adapter.filesystem.ListMetadata(collectionPath)
	if err != nil {
		return nil, err
	}

	result := make([]s3adminext.Metadata, 0, len(metadata))
	for _, avu := range metadata {
		if avu == nil {
			continue
		}
		result = append(result, s3adminext.Metadata{
			Name:  avu.Name,
			Value: avu.Value,
			Units: avu.Units,
		})
	}
	return result, nil
}

func (adapter *s3AdminFilesystemAdapter) AddCollectionMetadata(collectionPath string, metadata s3adminext.Metadata) error {
	return adapter.filesystem.AddMetadata(collectionPath, metadata.Name, metadata.Value, metadata.Units)
}

func (adapter *s3AdminFilesystemAdapter) DeleteCollectionMetadata(collectionPath string, metadata s3adminext.Metadata) error {
	metadataList, err := adapter.filesystem.ListMetadata(collectionPath)
	if err != nil {
		return err
	}

	for _, avu := range metadataList {
		if avu == nil {
			continue
		}
		if avu.Name == metadata.Name && avu.Value == metadata.Value && avu.Units == metadata.Units {
			return adapter.filesystem.DeleteMetadata(collectionPath, avu.AVUID)
		}
	}
	return irodstypes.NewFileNotFoundError(collectionPath)
}

func (adapter *s3AdminFilesystemAdapter) ReadDataObject(dataObjectPath string) ([]byte, error) {
	entry, err := adapter.filesystem.Stat(dataObjectPath)
	if err != nil {
		return nil, err
	}
	if entry == nil || entry.IsDir() {
		return nil, irodstypes.NewFileNotFoundError(dataObjectPath)
	}
	if entry.Size == 0 {
		return []byte{}, nil
	}

	handle, err := adapter.filesystem.OpenFile(dataObjectPath, "", "r")
	if err != nil {
		return nil, err
	}
	defer handle.Close() //nolint

	buffer := make([]byte, entry.Size)
	read, err := handle.ReadAt(buffer, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return buffer[:read], nil
}

func (adapter *s3AdminFilesystemAdapter) WriteDataObject(dataObjectPath string, contents []byte) error {
	if entry, err := adapter.filesystem.Stat(dataObjectPath); err == nil {
		if entry == nil || entry.IsDir() {
			return irodstypes.NewFileNotFoundError(dataObjectPath)
		}
		if err := adapter.filesystem.RemoveFile(dataObjectPath, true); err != nil {
			return err
		}
	} else if normalizedErr := normalizePathAccessError("stat data object", dataObjectPath, err); !errors.Is(normalizedErr, ErrNotFound) {
		return normalizedErr
	}

	handle, err := adapter.filesystem.CreateFile(dataObjectPath, "", "w")
	if err != nil {
		return err
	}
	if len(contents) > 0 {
		if _, err := handle.Write(contents); err != nil {
			handle.Close() //nolint
			return err
		}
	}
	return handle.Close()
}

func (adapter *s3AdminFilesystemAdapter) DeleteDataObject(dataObjectPath string, force bool) error {
	return adapter.filesystem.RemoveFile(dataObjectPath, force)
}

func (adapter *s3AdminFilesystemAdapter) ListDataObjectMetadata(dataObjectPath string) ([]s3adminext.Metadata, error) {
	return adapter.metadata(dataObjectPath)
}

func (adapter *s3AdminFilesystemAdapter) AddDataObjectMetadata(dataObjectPath string, metadata s3adminext.Metadata) error {
	return adapter.filesystem.AddMetadata(dataObjectPath, metadata.Name, metadata.Value, metadata.Units)
}

func (adapter *s3AdminFilesystemAdapter) DeleteDataObjectMetadata(dataObjectPath string, metadata s3adminext.Metadata) error {
	return adapter.deleteMetadata(dataObjectPath, metadata)
}

func (adapter *s3AdminFilesystemAdapter) metadata(irodsPath string) ([]s3adminext.Metadata, error) {
	metadata, err := adapter.filesystem.ListMetadata(irodsPath)
	if err != nil {
		return nil, err
	}

	result := make([]s3adminext.Metadata, 0, len(metadata))
	for _, avu := range metadata {
		if avu == nil {
			continue
		}
		result = append(result, s3adminext.Metadata{
			Name:  avu.Name,
			Value: avu.Value,
			Units: avu.Units,
		})
	}
	return result, nil
}

func (adapter *s3AdminFilesystemAdapter) deleteMetadata(irodsPath string, metadata s3adminext.Metadata) error {
	metadataList, err := adapter.filesystem.ListMetadata(irodsPath)
	if err != nil {
		return err
	}

	for _, avu := range metadataList {
		if avu == nil {
			continue
		}
		if avu.Name == metadata.Name && avu.Value == metadata.Value && avu.Units == metadata.Units {
			return adapter.filesystem.DeleteMetadata(irodsPath, avu.AVUID)
		}
	}
	return irodstypes.NewFileNotFoundError(irodsPath)
}
