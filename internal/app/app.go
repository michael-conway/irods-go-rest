package app

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/michael-conway/irods-go-rest/internal/config"
	"github.com/michael-conway/irods-go-rest/internal/httpapi"
	"github.com/michael-conway/irods-go-rest/internal/irods"
)

type App struct {
	server *http.Server
}

func New(cfg config.Config) *App {
	catalog := irods.NewCatalogService(cfg)
	handler := httpapi.NewHandler(cfg, catalog)

	return &App{
		server: &http.Server{
			Addr:              cfg.ServerAddr,
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
