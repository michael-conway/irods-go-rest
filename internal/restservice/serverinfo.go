package restservice

import (
	"context"

	"github.com/michael-conway/irods-go-rest/internal/domain"
	"github.com/michael-conway/irods-go-rest/internal/irods"
)

type ServerInfoService interface {
	GetServerInfo(ctx context.Context) (domain.ServerInfo, error)
}

type serverInfoService struct {
	service irods.ServerInfoService
}

func NewServerInfoService(service irods.ServerInfoService) ServerInfoService {
	return &serverInfoService{service: service}
}

func (s *serverInfoService) GetServerInfo(ctx context.Context) (domain.ServerInfo, error) {
	requestContext, err := RequestContextFromContext(ctx)
	if err != nil {
		return domain.ServerInfo{}, err
	}

	return s.service.GetServerInfo(ctx, irodsRequestContext(requestContext))
}
