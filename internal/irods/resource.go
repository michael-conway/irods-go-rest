package irods

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	irodsfs "github.com/cyverse/go-irodsclient/fs"
	irodstypes "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/michael-conway/irods-go-rest/internal/config"
	"github.com/michael-conway/irods-go-rest/internal/domain"
)

type ResourceService interface {
	ListResources(ctx context.Context, requestContext *RequestContext, scope string) ([]domain.Resource, error)
	GetResource(ctx context.Context, requestContext *RequestContext, resourceID string) (domain.Resource, error)
}

type resourceService struct {
	cfg              config.RestConfig
	createFileSystem CatalogFileSystemFactory
}

func NewResourceService(cfg config.RestConfig) ResourceService {
	return NewResourceServiceWithFactory(cfg, func(account *irodstypes.IRODSAccount, applicationName string) (CatalogFileSystem, error) {
		filesystem, err := irodsfs.NewFileSystemWithDefault(account, applicationName)
		if err != nil {
			return nil, err
		}
		return &catalogFileSystemAdapter{filesystem: filesystem}, nil
	})
}

func NewResourceServiceWithFactory(cfg config.RestConfig, factory CatalogFileSystemFactory) ResourceService {
	return &resourceService{
		cfg:              cfg,
		createFileSystem: factory,
	}
}

func (s *resourceService) ListResources(_ context.Context, requestContext *RequestContext, scope string) ([]domain.Resource, error) {
	catalog := &catalogService{
		cfg:              s.cfg,
		createFileSystem: s.createFileSystem,
	}

	filesystem, err := catalog.filesystemForRequest(requestContext, "irods-go-rest-list-resources")
	if err != nil {
		return nil, err
	}
	defer filesystem.Release()

	resources, err := filesystem.ListResources()
	if err != nil {
		return nil, err
	}

	result := make([]domain.Resource, 0, len(resources))
	for _, resource := range resources {
		if resource == nil {
			continue
		}

		result = append(result, mapResource(resource))
	}

	// The current go-irodsclient resource model is flat and does not expose
	// parent/child hierarchy fields, so "top" and "all" are equivalent until
	// that metadata is available upstream.
	_ = scope

	return result, nil
}

func (s *resourceService) GetResource(_ context.Context, requestContext *RequestContext, resourceID string) (domain.Resource, error) {
	resourceID = strings.TrimSpace(resourceID)
	if resourceID == "" {
		return domain.Resource{}, fmt.Errorf("%w: resource %q", ErrNotFound, resourceID)
	}

	catalog := &catalogService{
		cfg:              s.cfg,
		createFileSystem: s.createFileSystem,
	}

	filesystem, err := catalog.filesystemForRequest(requestContext, "irods-go-rest-get-resource")
	if err != nil {
		return domain.Resource{}, err
	}
	defer filesystem.Release()

	resource, err := filesystem.GetResource(resourceID)
	if err != nil {
		if irodstypes.IsResourceNotFoundError(err) {
			return domain.Resource{}, fmt.Errorf("%w: resource %q", ErrNotFound, resourceID)
		}
		return domain.Resource{}, err
	}

	return mapResource(resource), nil
}

func mapResource(resource *irodstypes.IRODSResource) domain.Resource {
	if resource == nil {
		return domain.Resource{}
	}

	name := strings.TrimSpace(resource.Name)
	return domain.Resource{
		ID:        resource.RescID,
		Name:      name,
		Zone:      strings.TrimSpace(resource.Zone),
		Type:      strings.TrimSpace(resource.Type),
		Class:     strings.TrimSpace(resource.Class),
		Location:  strings.TrimSpace(resource.Location),
		Path:      strings.TrimSpace(resource.Path),
		Context:   strings.TrimSpace(resource.Context),
		CreatedAt: timePointer(resource.CreateTime),
		UpdatedAt: timePointer(resource.ModifyTime),
		Links: &domain.ResourceLinks{
			Self: &domain.ActionLink{
				Href:   "/api/v1/resource/" + resourcePathEscape(name),
				Method: "GET",
			},
		},
	}
}

func resourcePathEscape(resourceID string) string {
	return url.PathEscape(strings.TrimSpace(resourceID))
}
