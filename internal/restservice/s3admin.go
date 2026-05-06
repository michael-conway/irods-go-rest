package restservice

import (
	"context"

	"github.com/michael-conway/irods-go-rest/internal/domain"
	"github.com/michael-conway/irods-go-rest/internal/irods"
)

type S3AdminService interface {
	ListBuckets(ctx context.Context, options irods.S3BucketListOptions) ([]domain.S3Bucket, error)
	GetBucket(ctx context.Context, bucketID string, options irods.S3BucketListOptions) (domain.S3Bucket, error)
	GetBucketByPath(ctx context.Context, irodsPath string) (domain.S3Bucket, error)
	UpsertBucket(ctx context.Context, irodsPath string, options irods.S3BucketUpsertOptions) (domain.S3Bucket, bool, error)
	DeleteBucket(ctx context.Context, bucketID string) error
	RebuildBucketMapping(ctx context.Context) (domain.S3BucketMappingRefresh, error)
}

type s3AdminService struct {
	catalog irods.CatalogService
}

func NewS3AdminService(catalog irods.CatalogService) S3AdminService {
	return &s3AdminService{catalog: catalog}
}

func (s *s3AdminService) ListBuckets(ctx context.Context, options irods.S3BucketListOptions) ([]domain.S3Bucket, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return nil, err
	}

	return s.catalog.ListS3Buckets(ctx, irodsRequestContext(requestContext), options)
}

func (s *s3AdminService) GetBucket(ctx context.Context, bucketID string, options irods.S3BucketListOptions) (domain.S3Bucket, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return domain.S3Bucket{}, err
	}

	return s.catalog.GetS3Bucket(ctx, irodsRequestContext(requestContext), bucketID, options)
}

func (s *s3AdminService) GetBucketByPath(ctx context.Context, irodsPath string) (domain.S3Bucket, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return domain.S3Bucket{}, err
	}

	return s.catalog.GetS3BucketByPath(ctx, irodsRequestContext(requestContext), irodsPath)
}

func (s *s3AdminService) UpsertBucket(ctx context.Context, irodsPath string, options irods.S3BucketUpsertOptions) (domain.S3Bucket, bool, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return domain.S3Bucket{}, false, err
	}

	return s.catalog.UpsertS3Bucket(ctx, irodsRequestContext(requestContext), irodsPath, options)
}

func (s *s3AdminService) DeleteBucket(ctx context.Context, bucketID string) error {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return err
	}

	return s.catalog.DeleteS3Bucket(ctx, irodsRequestContext(requestContext), bucketID)
}

func (s *s3AdminService) RebuildBucketMapping(ctx context.Context) (domain.S3BucketMappingRefresh, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return domain.S3BucketMappingRefresh{}, err
	}

	return s.catalog.RebuildS3BucketMapping(ctx, irodsRequestContext(requestContext))
}
