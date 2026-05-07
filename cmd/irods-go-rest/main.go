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
	log.Printf("starting irods-go-rest (startup marker: auth-config-debug-v2)")

	cfg, err := config.ReadRestConfig("rest-config", "yaml", []string{})

	if err != nil {
		log.Fatal(err)
	}

	log.Printf(
		"loaded config: public_url=%q listen_addr=%q oidc_url=%q oidc_realm=%q oidc_client_id=%q irods_host=%q irods_port=%d irods_admin_login_type=%q irods_auth_scheme=%q irods_negotiation_policy=%q s3_api_supported=%t",
		cfg.PublicURL,
		cfg.ListenAddr,
		cfg.OidcUrl,
		cfg.OidcRealm,
		cfg.OidcClientId,
		cfg.IrodsHost,
		cfg.IrodsPort,
		cfg.IrodsAdminLoginType,
		cfg.IrodsAuthScheme,
		cfg.IrodsNegotiationPolicy,
		cfg.S3ApiSupported,
	)

	application := app.New(*cfg)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := application.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
