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
	}
}

func getenv(key string, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	return value
}
