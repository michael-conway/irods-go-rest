package restservice

import (
	"context"

	"github.com/michael-conway/irods-go-rest/internal/domain"
	"github.com/michael-conway/irods-go-rest/internal/irods"
)

type ResourceService interface {
	ListResources(ctx context.Context, scope string) ([]domain.Resource, error)
	GetResource(ctx context.Context, resourceID string) (domain.Resource, error)
}

type resourceService struct {
	resources irods.ResourceService
}

func NewResourceService(resources irods.ResourceService) ResourceService {
	return &resourceService{resources: resources}
}

func (s *resourceService) ListResources(ctx context.Context, scope string) ([]domain.Resource, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return nil, err
	}

	return s.resources.ListResources(ctx, irodsRequestContext(requestContext), scope)
}

func (s *resourceService) GetResource(ctx context.Context, resourceID string) (domain.Resource, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return domain.Resource{}, err
	}

	return s.resources.GetResource(ctx, irodsRequestContext(requestContext), resourceID)
}
