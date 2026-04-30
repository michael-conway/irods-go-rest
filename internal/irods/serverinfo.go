package irods

import (
	"context"
	"strings"

	irodsfs "github.com/cyverse/go-irodsclient/fs"
	irodstypes "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/michael-conway/irods-go-rest/internal/config"
	"github.com/michael-conway/irods-go-rest/internal/domain"
)

type ServerInfoService interface {
	GetServerInfo(ctx context.Context, requestContext *RequestContext) (domain.ServerInfo, error)
}

type serverInfoService struct {
	cfg              config.RestConfig
	createFileSystem CatalogFileSystemFactory
}

func NewServerInfoService(cfg config.RestConfig) ServerInfoService {
	return NewServerInfoServiceWithFactory(cfg, func(account *irodstypes.IRODSAccount, applicationName string) (CatalogFileSystem, error) {
		filesystem, err := irodsfs.NewFileSystemWithDefault(account, applicationName)
		if err != nil {
			return nil, err
		}
		return &catalogFileSystemAdapter{filesystem: filesystem}, nil
	})
}

func NewServerInfoServiceWithFactory(cfg config.RestConfig, factory CatalogFileSystemFactory) ServerInfoService {
	return &serverInfoService{
		cfg:              cfg,
		createFileSystem: factory,
	}
}

func (s *serverInfoService) GetServerInfo(_ context.Context, requestContext *RequestContext) (domain.ServerInfo, error) {
	catalog := &catalogService{
		cfg:              s.cfg,
		createFileSystem: s.createFileSystem,
	}

	filesystem, err := catalog.filesystemForRequest(requestContext, "irods-go-rest-get-server-info")
	if err != nil {
		return domain.ServerInfo{}, err
	}
	defer filesystem.Release()

	version, err := filesystem.GetServerVersion()
	if err != nil {
		return domain.ServerInfo{}, err
	}

	resourceAffinity := make([]string, 0, len(s.cfg.ResourceAffinity))
	for _, entry := range s.cfg.ResourceAffinity {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		resourceAffinity = append(resourceAffinity, entry)
	}

	response := domain.ServerInfo{
		IRODSHost:            strings.TrimSpace(s.cfg.IrodsHost),
		IRODSPort:            s.cfg.IrodsPort,
		IRODSZone:            strings.TrimSpace(s.cfg.IrodsZone),
		IRODSNegotiation:     effectiveNegotiationPolicy(s.cfg.IrodsNegotiationPolicy),
		IRODSDefaultResource: strings.TrimSpace(s.cfg.IrodsDefaultResource),
		ResourceAffinity:     resourceAffinity,
	}

	if version != nil {
		response.ReleaseVersion = strings.TrimSpace(version.ReleaseVersion)
		response.APIVersion = strings.TrimSpace(version.APIVersion)
		response.ReconnectPort = version.ReconnectPort
		response.ReconnectAddr = strings.TrimSpace(version.ReconnectAddr)
		response.Cookie = version.Cookie
	}

	return response, nil
}

func effectiveNegotiationPolicy(policy string) string {
	policy = strings.TrimSpace(policy)
	if policy == "" {
		return "native"
	}
	return policy
}
