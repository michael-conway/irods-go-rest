package restservice

import (
	"context"

	irodstypes "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/michael-conway/irods-go-rest/internal/domain"
	"github.com/michael-conway/irods-go-rest/internal/irods"
)

type PathService interface {
	GetPath(ctx context.Context, absolutePath string, options irods.PathLookupOptions) (domain.PathEntry, error)
	GetPathChildren(ctx context.Context, absolutePath string) ([]domain.PathEntry, error)
	SearchPathChildren(ctx context.Context, absolutePath string, options irods.PathChildrenListOptions) (irods.PathChildrenSearchResult, error)
	GetPathReplicas(ctx context.Context, absolutePath string, verboseLevel int) ([]domain.PathReplica, error)
	UploadPathContents(ctx context.Context, absolutePath string, options irods.PathContentsUploadOptions) (domain.PathContentsUploadResult, error)
	CreatePathChild(ctx context.Context, absolutePath string, options irods.PathCreateOptions) (domain.PathEntry, error)
	CreatePathReplica(ctx context.Context, absolutePath string, options irods.PathReplicaCreateOptions) ([]domain.PathReplica, error)
	MovePathReplica(ctx context.Context, absolutePath string, options irods.PathReplicaMoveOptions) ([]domain.PathReplica, error)
	TrimPathReplica(ctx context.Context, absolutePath string, options irods.PathReplicaTrimOptions) ([]domain.PathReplica, error)
	DeletePath(ctx context.Context, absolutePath string, force bool) error
	RenamePath(ctx context.Context, absolutePath string, newName string) (domain.PathEntry, error)
	GetPathMetadata(ctx context.Context, absolutePath string) ([]domain.AVUMetadata, error)
	AddPathMetadata(ctx context.Context, absolutePath string, attrib string, value string, unit string) (domain.AVUMetadata, error)
	UpdatePathMetadata(ctx context.Context, absolutePath string, avuID string, attrib string, value string, unit string) (domain.AVUMetadata, error)
	DeletePathMetadata(ctx context.Context, absolutePath string, avuID string) error
	GetPathACL(ctx context.Context, absolutePath string) (domain.PathACL, error)
	AddPathACL(ctx context.Context, absolutePath string, acl irodstypes.IRODSAccess, recursive bool) (domain.PathACLEntry, error)
	UpdatePathACL(ctx context.Context, absolutePath string, aclID string, accessLevel string, recursive bool) (domain.PathACLEntry, error)
	DeletePathACL(ctx context.Context, absolutePath string, aclID string) error
	SetPathACLInheritance(ctx context.Context, absolutePath string, enabled bool, recursive bool) error
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

func (s *pathService) SearchPathChildren(ctx context.Context, absolutePath string, options irods.PathChildrenListOptions) (irods.PathChildrenSearchResult, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return irods.PathChildrenSearchResult{}, err
	}

	return s.catalog.SearchPathChildren(ctx, irodsRequestContext(requestContext), absolutePath, options)
}

func (s *pathService) GetPathReplicas(ctx context.Context, absolutePath string, verboseLevel int) ([]domain.PathReplica, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return nil, err
	}

	return s.catalog.GetPathReplicas(ctx, irodsRequestContext(requestContext), absolutePath, verboseLevel)
}

func (s *pathService) UploadPathContents(ctx context.Context, absolutePath string, options irods.PathContentsUploadOptions) (domain.PathContentsUploadResult, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return domain.PathContentsUploadResult{}, err
	}

	return s.catalog.UploadPathContents(ctx, irodsRequestContext(requestContext), absolutePath, options)
}

func (s *pathService) CreatePathChild(ctx context.Context, absolutePath string, options irods.PathCreateOptions) (domain.PathEntry, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return domain.PathEntry{}, err
	}

	return s.catalog.CreatePathChild(ctx, irodsRequestContext(requestContext), absolutePath, options)
}

func (s *pathService) CreatePathReplica(ctx context.Context, absolutePath string, options irods.PathReplicaCreateOptions) ([]domain.PathReplica, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return nil, err
	}

	return s.catalog.CreatePathReplica(ctx, irodsRequestContext(requestContext), absolutePath, options)
}

func (s *pathService) MovePathReplica(ctx context.Context, absolutePath string, options irods.PathReplicaMoveOptions) ([]domain.PathReplica, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return nil, err
	}

	return s.catalog.MovePathReplica(ctx, irodsRequestContext(requestContext), absolutePath, options)
}

func (s *pathService) TrimPathReplica(ctx context.Context, absolutePath string, options irods.PathReplicaTrimOptions) ([]domain.PathReplica, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return nil, err
	}

	return s.catalog.TrimPathReplica(ctx, irodsRequestContext(requestContext), absolutePath, options)
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

func (s *pathService) GetPathACL(ctx context.Context, absolutePath string) (domain.PathACL, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return domain.PathACL{}, err
	}

	return s.catalog.GetPathACL(ctx, irodsRequestContext(requestContext), absolutePath)
}

func (s *pathService) AddPathACL(ctx context.Context, absolutePath string, acl irodstypes.IRODSAccess, recursive bool) (domain.PathACLEntry, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return domain.PathACLEntry{}, err
	}

	return s.catalog.AddPathACL(ctx, irodsRequestContext(requestContext), absolutePath, acl, recursive)
}

func (s *pathService) UpdatePathACL(ctx context.Context, absolutePath string, aclID string, accessLevel string, recursive bool) (domain.PathACLEntry, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return domain.PathACLEntry{}, err
	}

	return s.catalog.UpdatePathACL(ctx, irodsRequestContext(requestContext), absolutePath, aclID, accessLevel, recursive)
}

func (s *pathService) DeletePathACL(ctx context.Context, absolutePath string, aclID string) error {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return err
	}

	return s.catalog.DeletePathACL(ctx, irodsRequestContext(requestContext), absolutePath, aclID)
}

func (s *pathService) SetPathACLInheritance(ctx context.Context, absolutePath string, enabled bool, recursive bool) error {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return err
	}

	return s.catalog.SetPathACLInheritance(ctx, irodsRequestContext(requestContext), absolutePath, enabled, recursive)
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
