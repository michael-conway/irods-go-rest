package irods

import (
	"context"
	"errors"
	"fmt"
	"mime"
	"path/filepath"
	"strings"

	"github.com/michael-conway/irods-go-rest/internal/config"
	"github.com/michael-conway/irods-go-rest/internal/domain"
)

var ErrNotFound = errors.New("resource not found")

type CatalogService interface {
	GetObject(ctx context.Context, objectID string) (domain.Object, error)
	GetObjectByPath(ctx context.Context, absolutePath string) (domain.Object, error)
	GetObjectContentByPath(ctx context.Context, absolutePath string) (domain.ObjectContent, error)
	GetCollection(ctx context.Context, collectionID string) (domain.Collection, error)
}

type catalogService struct {
	cfg config.RestConfig
}

func NewCatalogService(cfg config.RestConfig) CatalogService {
	return &catalogService{cfg: cfg}
}

func (s *catalogService) GetObject(_ context.Context, objectID string) (domain.Object, error) {
	if objectID == "missing" {
		return domain.Object{}, fmt.Errorf("%w: object %q", ErrNotFound, objectID)
	}

	return domain.Object{
		ID:       objectID,
		Path:     fmt.Sprintf("/%s/home/rods/%s", s.cfg.IrodsZone, objectID),
		Checksum: "sha256:demo",
		Size:     1024,
		Zone:     s.cfg.IrodsZone,
		Resource: s.cfg.IrodsDefaultResource,
		Metadata: map[string]string{
			"source": "scaffold",
		},
	}, nil
}

func (s *catalogService) GetObjectByPath(_ context.Context, absolutePath string) (domain.Object, error) {
	absolutePath = strings.TrimSpace(absolutePath)
	if absolutePath == "" || absolutePath == "missing" {
		return domain.Object{}, fmt.Errorf("%w: object %q", ErrNotFound, absolutePath)
	}

	return domain.Object{
		ID:       absolutePath,
		Path:     absolutePath,
		Checksum: "sha256:demo",
		Size:     int64(len([]byte("demo content for " + absolutePath))),
		Zone:     s.cfg.IrodsZone,
		Resource: s.cfg.IrodsDefaultResource,
		Metadata: map[string]string{
			"source": "scaffold",
		},
	}, nil
}

func (s *catalogService) GetObjectContentByPath(_ context.Context, absolutePath string) (domain.ObjectContent, error) {
	absolutePath = strings.TrimSpace(absolutePath)
	if absolutePath == "" || absolutePath == "missing" {
		return domain.ObjectContent{}, fmt.Errorf("%w: object %q", ErrNotFound, absolutePath)
	}

	contentType := mime.TypeByExtension(filepath.Ext(absolutePath))
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	data := []byte("demo content for " + absolutePath)
	return domain.ObjectContent{
		Path:        absolutePath,
		ContentType: contentType,
		Size:        int64(len(data)),
		Data:        data,
	}, nil
}

func (s *catalogService) GetCollection(_ context.Context, collectionID string) (domain.Collection, error) {
	if collectionID == "missing" {
		return domain.Collection{}, fmt.Errorf("%w: collection %q", ErrNotFound, collectionID)
	}

	return domain.Collection{
		ID:         collectionID,
		Path:       fmt.Sprintf("/%s/home/rods/%s", s.cfg.IrodsZone, collectionID),
		Zone:       s.cfg.IrodsZone,
		ChildCount: 3,
		Metadata: map[string]string{
			"source": "scaffold",
		},
	}, nil
}
