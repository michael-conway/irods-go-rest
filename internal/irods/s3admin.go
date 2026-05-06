package irods

import (
	"context"
	"errors"
	"fmt"
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

func (s *catalogService) prepareS3AdminService(requestContext *RequestContext, applicationName string) (CatalogFileSystem, *s3adminext.S3Service, error) {
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

	service, err := s3adminext.NewS3ServiceWithMappingFile(&s3AdminFilesystemAdapter{filesystem: filesystem}, scanRoot, s.s3BucketMappingFile)
	if err != nil {
		filesystem.Release()
		return nil, nil, normalizeS3AdminError("create s3admin service", scanRoot, err)
	}

	return filesystem, service, nil
}

func normalizeS3AdminError(operation string, irodsPath string, err error) error {
	if err == nil {
		return nil
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

type s3AdminFilesystemAdapter struct {
	filesystem CatalogFileSystem
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
