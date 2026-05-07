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
	ListUserSecrets(ctx context.Context) ([]domain.S3UserSecret, error)
	GetUserSecret(ctx context.Context, userName string) (domain.S3UserSecret, error)
	StoreUserSecret(ctx context.Context, userName string, options irods.S3UserSecretStoreOptions) (domain.S3UserSecret, error)
	DeleteUserSecret(ctx context.Context, userName string) error
	RebuildUserMapping(ctx context.Context) (domain.S3UserSecretMappingRefresh, error)
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

func (s *s3AdminService) ListUserSecrets(ctx context.Context) ([]domain.S3UserSecret, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return nil, err
	}

	return s.catalog.ListS3UserSecrets(ctx, irodsRequestContext(requestContext))
}

func (s *s3AdminService) GetUserSecret(ctx context.Context, userName string) (domain.S3UserSecret, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return domain.S3UserSecret{}, err
	}

	return s.catalog.GetS3UserSecret(ctx, irodsRequestContext(requestContext), userName)
}

func (s *s3AdminService) StoreUserSecret(ctx context.Context, userName string, options irods.S3UserSecretStoreOptions) (domain.S3UserSecret, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return domain.S3UserSecret{}, err
	}

	return s.catalog.StoreS3UserSecret(ctx, irodsRequestContext(requestContext), userName, options)
}

func (s *s3AdminService) DeleteUserSecret(ctx context.Context, userName string) error {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return err
	}

	return s.catalog.DeleteS3UserSecret(ctx, irodsRequestContext(requestContext), userName)
}

func (s *s3AdminService) RebuildUserMapping(ctx context.Context) (domain.S3UserSecretMappingRefresh, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return domain.S3UserSecretMappingRefresh{}, err
	}

	return s.catalog.RebuildS3UserMapping(ctx, irodsRequestContext(requestContext))
}
