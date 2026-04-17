package config

import "os"

type Config struct {
	ServerAddr      string
	ServiceName     string
	Environment     string
	Zone            string
	Host            string
	Port            string
	DefaultResource string
	KeycloakURL     string
	KeycloakRealm   string
	KeycloakClient  string
	KeycloakSecret  string
	PublicURL       string
	AuthScopes      string
}

func FromEnv() Config {
	return Config{
		ServerAddr:      getenv("IRODS_REST_ADDR", ":8080"),
		ServiceName:     getenv("IRODS_REST_NAME", "iRODS REST API"),
		Environment:     getenv("IRODS_REST_ENV", "development"),
		Zone:            getenv("IRODS_ZONE", "tempZone"),
		Host:            getenv("IRODS_HOST", "localhost"),
		Port:            getenv("IRODS_PORT", "1247"),
		DefaultResource: getenv("IRODS_DEFAULT_RESOURCE", "demoResc"),
		KeycloakURL:     getenv("KEYCLOAK_URL", ""),
		KeycloakRealm:   getenv("KEYCLOAK_REALM", ""),
		KeycloakClient:  getenv("KEYCLOAK_CLIENT_ID", ""),
		KeycloakSecret:  getenv("KEYCLOAK_CLIENT_SECRET", ""),
		PublicURL:       getenv("IRODS_REST_PUBLIC_URL", "http://localhost:8080"),
		AuthScopes:      getenv("KEYCLOAK_SCOPES", "openid profile email"),
	}
}

func getenv(key string, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	return value
}
