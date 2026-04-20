package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/michael-conway/irods-go-rest/internal/app"
	"github.com/michael-conway/irods-go-rest/internal/config"
)

func main() {
	cfg, err := config.ReadRestConfig("rest-config", "yaml", []string{})

	if err != nil {
		log.Fatal(err)
	}

	application := app.New(*cfg)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := application.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
