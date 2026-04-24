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
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return domain.PathEntry{}, err
	}

	return s.catalog.GetPath(ctx, irodsRequestContext(requestContext), absolutePath)
}

func (s *pathService) GetPathChildren(ctx context.Context, absolutePath string) ([]domain.PathEntry, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return nil, err
	}

	return s.catalog.GetPathChildren(ctx, irodsRequestContext(requestContext), absolutePath)
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
