package restservice

import (
	"context"

	"github.com/michael-conway/irods-go-rest/internal/domain"
	"github.com/michael-conway/irods-go-rest/internal/irods"
)

type PathService interface {
	GetPath(ctx context.Context, absolutePath string, options irods.PathLookupOptions) (domain.PathEntry, error)
	GetPathChildren(ctx context.Context, absolutePath string) ([]domain.PathEntry, error)
	CreatePathChild(ctx context.Context, absolutePath string, options irods.PathCreateOptions) (domain.PathEntry, error)
	DeletePath(ctx context.Context, absolutePath string, force bool) error
	RenamePath(ctx context.Context, absolutePath string, newName string) (domain.PathEntry, error)
	GetPathMetadata(ctx context.Context, absolutePath string) ([]domain.AVUMetadata, error)
	AddPathMetadata(ctx context.Context, absolutePath string, attrib string, value string, unit string) (domain.AVUMetadata, error)
	UpdatePathMetadata(ctx context.Context, absolutePath string, avuID string, attrib string, value string, unit string) (domain.AVUMetadata, error)
	DeletePathMetadata(ctx context.Context, absolutePath string, avuID string) error
	GetPathChecksum(ctx context.Context, absolutePath string) (domain.PathChecksum, error)
	ComputePathChecksum(ctx context.Context, absolutePath string) (domain.PathChecksum, error)
	GetObjectContentByPath(ctx context.Context, absolutePath string) (domain.ObjectContent, error)
}

type pathService struct {
	catalog irods.CatalogService
}

func NewPathService(catalog irods.CatalogService) PathService {
	return &pathService{catalog: catalog}
}

func (s *pathService) GetPath(ctx context.Context, absolutePath string, options irods.PathLookupOptions) (domain.PathEntry, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return domain.PathEntry{}, err
	}

	return s.catalog.GetPath(ctx, irodsRequestContext(requestContext), absolutePath, options)
}

func (s *pathService) GetPathChildren(ctx context.Context, absolutePath string) ([]domain.PathEntry, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return nil, err
	}

	return s.catalog.GetPathChildren(ctx, irodsRequestContext(requestContext), absolutePath)
}

func (s *pathService) CreatePathChild(ctx context.Context, absolutePath string, options irods.PathCreateOptions) (domain.PathEntry, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return domain.PathEntry{}, err
	}

	return s.catalog.CreatePathChild(ctx, irodsRequestContext(requestContext), absolutePath, options)
}

func (s *pathService) DeletePath(ctx context.Context, absolutePath string, force bool) error {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return err
	}

	return s.catalog.DeletePath(ctx, irodsRequestContext(requestContext), absolutePath, force)
}

func (s *pathService) RenamePath(ctx context.Context, absolutePath string, newName string) (domain.PathEntry, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return domain.PathEntry{}, err
	}

	return s.catalog.RenamePath(ctx, irodsRequestContext(requestContext), absolutePath, newName)
}

func (s *pathService) GetPathMetadata(ctx context.Context, absolutePath string) ([]domain.AVUMetadata, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return nil, err
	}

	return s.catalog.GetPathMetadata(ctx, irodsRequestContext(requestContext), absolutePath)
}

func (s *pathService) AddPathMetadata(ctx context.Context, absolutePath string, attrib string, value string, unit string) (domain.AVUMetadata, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return domain.AVUMetadata{}, err
	}

	return s.catalog.AddPathMetadata(ctx, irodsRequestContext(requestContext), absolutePath, attrib, value, unit)
}

func (s *pathService) UpdatePathMetadata(ctx context.Context, absolutePath string, avuID string, attrib string, value string, unit string) (domain.AVUMetadata, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return domain.AVUMetadata{}, err
	}

	return s.catalog.UpdatePathMetadata(ctx, irodsRequestContext(requestContext), absolutePath, avuID, attrib, value, unit)
}

func (s *pathService) DeletePathMetadata(ctx context.Context, absolutePath string, avuID string) error {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return err
	}

	return s.catalog.DeletePathMetadata(ctx, irodsRequestContext(requestContext), absolutePath, avuID)
}

func (s *pathService) GetPathChecksum(ctx context.Context, absolutePath string) (domain.PathChecksum, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return domain.PathChecksum{}, err
	}

	return s.catalog.GetPathChecksum(ctx, irodsRequestContext(requestContext), absolutePath)
}

func (s *pathService) ComputePathChecksum(ctx context.Context, absolutePath string) (domain.PathChecksum, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return domain.PathChecksum{}, err
	}

	return s.catalog.ComputePathChecksum(ctx, irodsRequestContext(requestContext), absolutePath)
}

func (s *pathService) GetObjectContentByPath(ctx context.Context, absolutePath string) (domain.ObjectContent, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return domain.ObjectContent{}, err
	}

	return s.catalog.GetObjectContentByPath(ctx, irodsRequestContext(requestContext), absolutePath)
}

func irodsRequestContext(requestContext *RequestContext) *irods.RequestContext {
	if requestContext == nil {
		return nil
	}

	username := ""
	if requestContext.Principal != nil {
		username = requestContext.Principal.Username
		if username == "" {
			username = requestContext.Principal.Subject
		}
	}

	return &irods.RequestContext{
		AuthScheme:    requestContext.AuthScheme,
		Username:      username,
		BasicPassword: requestContext.BasicPassword,
		Ticket:        requestContext.Ticket,
	}
}
