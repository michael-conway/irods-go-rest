package restservice

import (
	"context"

	"github.com/michael-conway/irods-go-rest/internal/domain"
	"github.com/michael-conway/irods-go-rest/internal/irods"
)

type PathService interface {
	GetPath(ctx context.Context, absolutePath string) (domain.PathEntry, error)
	GetPathChildren(ctx context.Context, absolutePath string) ([]domain.PathEntry, error)
	GetObjectContentByPath(ctx context.Context, absolutePath string) (domain.ObjectContent, error)
}

type pathService struct {
	catalog irods.CatalogService
}

func NewPathService(catalog irods.CatalogService) PathService {
	return &pathService{catalog: catalog}
}

func (s *pathService) GetPath(ctx context.Context, absolutePath string) (domain.PathEntry, error) {
	_, _ = RequestContextFromContext(ctx)
	return s.catalog.GetPath(ctx, absolutePath)
}

func (s *pathService) GetPathChildren(ctx context.Context, absolutePath string) ([]domain.PathEntry, error) {
	_, _ = RequestContextFromContext(ctx)
	return s.catalog.GetPathChildren(ctx, absolutePath)
}

func (s *pathService) GetObjectContentByPath(ctx context.Context, absolutePath string) (domain.ObjectContent, error) {
	_, _ = RequestContextFromContext(ctx)
	return s.catalog.GetObjectContentByPath(ctx, absolutePath)
}
