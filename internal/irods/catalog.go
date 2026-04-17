package irods

import (
	"context"
	"errors"
	"fmt"

	"github.com/michael-conway/irods-go-rest/internal/config"
	"github.com/michael-conway/irods-go-rest/internal/domain"
)

var ErrNotFound = errors.New("resource not found")

type CatalogService interface {
	GetObject(ctx context.Context, objectID string) (domain.Object, error)
	GetCollection(ctx context.Context, collectionID string) (domain.Collection, error)
}

type catalogService struct {
	cfg config.Config
}

func NewCatalogService(cfg config.Config) CatalogService {
	return &catalogService{cfg: cfg}
}

func (s *catalogService) GetObject(_ context.Context, objectID string) (domain.Object, error) {
	if objectID == "missing" {
		return domain.Object{}, fmt.Errorf("%w: object %q", ErrNotFound, objectID)
	}

	return domain.Object{
		ID:       objectID,
		Path:     fmt.Sprintf("/%s/home/rods/%s", s.cfg.Zone, objectID),
		Checksum: "sha256:demo",
		Size:     1024,
		Zone:     s.cfg.Zone,
		Resource: s.cfg.DefaultResource,
		Metadata: map[string]string{
			"source": "scaffold",
		},
	}, nil
}

func (s *catalogService) GetCollection(_ context.Context, collectionID string) (domain.Collection, error) {
	if collectionID == "missing" {
		return domain.Collection{}, fmt.Errorf("%w: collection %q", ErrNotFound, collectionID)
	}

	return domain.Collection{
		ID:         collectionID,
		Path:       fmt.Sprintf("/%s/home/rods/%s", s.cfg.Zone, collectionID),
		Zone:       s.cfg.Zone,
		ChildCount: 3,
		Metadata: map[string]string{
			"source": "scaffold",
		},
	}, nil
}
