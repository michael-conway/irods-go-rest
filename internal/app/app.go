package app

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/michael-conway/irods-go-rest/internal/auth"
	"github.com/michael-conway/irods-go-rest/internal/config"
	"github.com/michael-conway/irods-go-rest/internal/httpapi"
	"github.com/michael-conway/irods-go-rest/internal/irods"
	"github.com/michael-conway/irods-go-rest/internal/restservice"
)

type App struct {
	server *http.Server
}

func publicURLListenAddr(publicURL string) string {
	publicURL = strings.TrimSpace(publicURL)
	if publicURL == "" {
		return ""
	}

	parsedURL, err := url.Parse(publicURL)
	if err == nil && parsedURL.Host != "" {
		return parsedURL.Host
	}

	return publicURL
}

func serverListenAddr(cfg config.RestConfig) string {
	if listenAddr := strings.TrimSpace(cfg.ListenAddr); listenAddr != "" {
		return listenAddr
	}

	return publicURLListenAddr(cfg.PublicURL)
}

func New(cfg config.RestConfig) *App {
	catalog := irods.NewCatalogService(cfg)
	paths := restservice.NewPathService(catalog)
	s3Admin := restservice.NewS3AdminService(catalog)
	serverInfo := restservice.NewServerInfoService(irods.NewServerInfoService(cfg))
	resources := restservice.NewResourceService(irods.NewResourceService(cfg))
	users := restservice.NewUserService(irods.NewUserService(cfg))
	userGroups := restservice.NewUserGroupService(irods.NewUserGroupService(cfg))
	tickets := restservice.NewTicketService(irods.NewTicketService(cfg))
	authService := auth.NewKeycloakService(cfg)
	sessionStore := auth.NewSessionStore()
	handler := httpapi.NewHandler(cfg, paths, s3Admin, serverInfo, resources, users, userGroups, tickets, authService, authService, sessionStore)

	return &App{
		server: &http.Server{
			Addr:              serverListenAddr(cfg),
			Handler:           handler.Routes(),
			ReadHeaderTimeout: 5 * time.Second,
		},
	}
}

func (a *App) Run(ctx context.Context) error {
	errCh := make(chan error, 1)

	go func() {
		if err := a.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("http server failed: %w", err)
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := a.server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("http server shutdown failed: %w", err)
		}

		return nil
	case err := <-errCh:
		return err
	}
}
