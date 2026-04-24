package irods

import (
	"context"
	"errors"
	"fmt"
	"mime"
	"path"
	"path/filepath"
	"strings"

	"github.com/michael-conway/irods-go-rest/internal/config"
	"github.com/michael-conway/irods-go-rest/internal/domain"
)

var ErrNotFound = errors.New("resource not found")

type CatalogService interface {
	GetPath(ctx context.Context, absolutePath string) (domain.PathEntry, error)
	GetPathChildren(ctx context.Context, absolutePath string) ([]domain.PathEntry, error)
	GetObjectContentByPath(ctx context.Context, absolutePath string) (domain.ObjectContent, error)
}

type catalogService struct {
	cfg config.RestConfig
}

func NewCatalogService(cfg config.RestConfig) CatalogService {
	return &catalogService{cfg: cfg}
}

func (s *catalogService) GetPath(_ context.Context, absolutePath string) (domain.PathEntry, error) {
	absolutePath = strings.TrimSpace(absolutePath)
	if absolutePath == "" || absolutePath == "missing" {
		return domain.PathEntry{}, fmt.Errorf("%w: path %q", ErrNotFound, absolutePath)
	}

	if looksLikeCollection(absolutePath) {
		return domain.PathEntry{
			ID:          absolutePath,
			Path:        absolutePath,
			Kind:        "collection",
			Zone:        s.cfg.IrodsZone,
			HasChildren: true,
			ChildCount:  2,
			Metadata: map[string]string{
				"source": "scaffold",
			},
		}, nil
	}

	return domain.PathEntry{
		ID:       absolutePath,
		Path:     absolutePath,
		Kind:     "data_object",
		Checksum: "sha256:demo",
		Size:     int64(len([]byte("demo content for " + absolutePath))),
		Zone:     s.cfg.IrodsZone,
		Resource: s.cfg.IrodsDefaultResource,
		Metadata: map[string]string{
			"source": "scaffold",
		},
	}, nil
}

func (s *catalogService) GetPathChildren(_ context.Context, absolutePath string) ([]domain.PathEntry, error) {
	absolutePath = strings.TrimSpace(absolutePath)
	if absolutePath == "" || absolutePath == "missing" {
		return nil, fmt.Errorf("%w: path %q", ErrNotFound, absolutePath)
	}

	if !looksLikeCollection(absolutePath) {
		return nil, fmt.Errorf("%w: path %q is not a collection", ErrNotFound, absolutePath)
	}

	return []domain.PathEntry{
		{
			ID:   absolutePath + "/child.txt",
			Path: absolutePath + "/child.txt",
			Kind: "data_object",
			Zone: s.cfg.IrodsZone,
			Size: 128,
		},
		{
			ID:          absolutePath + "/nested",
			Path:        absolutePath + "/nested",
			Kind:        "collection",
			Zone:        s.cfg.IrodsZone,
			HasChildren: true,
			ChildCount:  1,
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

func looksLikeCollection(absolutePath string) bool {
	name := path.Base(strings.TrimRight(absolutePath, "/"))
	return !strings.Contains(name, ".")
}
