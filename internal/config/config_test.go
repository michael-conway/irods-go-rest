package config

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTestFile(t *testing.T, dir string, name string, contents string) string {
	t.Helper()

	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatalf("failed to write %s: %v", path, err)
	}

	return path
}

func TestReadRestConfigEnvOverride(t *testing.T) {
	dir := t.TempDir()
	writeTestFile(t, dir, "rest-config.yaml", "IrodsHost: localhost\nOidcClientSecret: file-secret\nRestLogLevel: info\n")

	t.Setenv("GOREST_IRODS_HOST", "env-host")
	t.Setenv("GOREST_OIDC_CLIENT_SECRET", "env-secret")
	t.Setenv("GOREST_REST_LOG_LEVEL", "debug")

	cfg, err := ReadRestConfig("rest-config", "yaml", []string{dir})
	if err != nil {
		t.Fatalf("error reading config: %v", err)
	}

	if cfg.IrodsHost != "env-host" {
		t.Fatalf("expected env override for IrodsHost, got %q", cfg.IrodsHost)
	}

	if cfg.OidcClientSecret != "env-secret" {
		t.Fatalf("expected env override for OidcClientSecret, got %q", cfg.OidcClientSecret)
	}

	if cfg.RestLogLevel != "debug" {
		t.Fatalf("expected env override for RestLogLevel, got %q", cfg.RestLogLevel)
	}
}

func TestReadRestConfigMissingFileReturnsError(t *testing.T) {
	_, err := ReadRestConfig("does-not-exist", "yaml", []string{t.TempDir()})
	if err == nil {
		t.Fatal("expected error for missing config file")
	}
}

func TestReadRestConfigSecretFileSupport(t *testing.T) {
	dir := t.TempDir()
	irodsSecretPath := writeTestFile(t, dir, "irods-admin-password.txt", "rods\n")
	oidcSecretPath := writeTestFile(t, dir, "oidc-client-secret.txt", "test-oidc-secret\n")

	configBody := "" +
		"IrodsHost: localhost\n" +
		"IrodsPort: 1247\n" +
		"IrodsZone: tempZone\n" +
		"IrodsAdminUser: rods\n" +
		"IrodsAdminPasswordFile: " + irodsSecretPath + "\n" +
		"IrodsAuthScheme: native\n" +
		"IrodsNegotiationPolicy: native\n" +
		"OidcUrl: https://localhost:8443\n" +
		"OidcClientSecretFile: " + oidcSecretPath + "\n" +
		"RestLogLevel: info\n"

	writeTestFile(t, dir, "rest-config.yaml", configBody)

	cfg, err := ReadRestConfig("rest-config", "yaml", []string{dir})
	if err != nil {
		t.Fatalf("error reading config: %v", err)
	}

	if cfg.IrodsAdminPassword != "rods" {
		t.Fatalf("expected secret file value for IrodsAdminPassword, got %q", cfg.IrodsAdminPassword)
	}

	if cfg.OidcClientSecret != "test-oidc-secret" {
		t.Fatalf("expected secret file value for OidcClientSecret, got %q", cfg.OidcClientSecret)
	}
}

func TestReadRestConfigConfigFileEnvOverride(t *testing.T) {
	dir := t.TempDir()
	configBody := "" +
		"IrodsHost: env-file-host\n" +
		"IrodsPort: 1247\n" +
		"IrodsZone: tempZone\n" +
		"IrodsAdminUser: rods\n" +
		"IrodsAuthScheme: native\n" +
		"IrodsNegotiationPolicy: native\n" +
		"PublicURL: http://env-file.example\n" +
		"RestLogLevel: info\n"
	configPath := writeTestFile(t, dir, "custom-rest-config.yaml", configBody)

	t.Setenv(ConfigFileEnvVar, configPath)

	cfg, err := ReadRestConfig("does-not-exist", "yaml", []string{t.TempDir()})
	if err != nil {
		t.Fatalf("error reading config with %s override: %v", ConfigFileEnvVar, err)
	}

	if cfg.PublicURL != "http://env-file.example" {
		t.Fatalf("expected PublicURL from %s override, got %q", ConfigFileEnvVar, cfg.PublicURL)
	}

	if cfg.IrodsHost != "env-file-host" {
		t.Fatalf("expected IrodsHost from %s override, got %q", ConfigFileEnvVar, cfg.IrodsHost)
	}
}

func TestReadRestConfigTrimsWhitespaceFromInputs(t *testing.T) {
	dir := t.TempDir()
	configBody := "" +
		"IrodsHost: trimmed-host\n" +
		"IrodsPort: 1247\n" +
		"IrodsZone: tempZone\n" +
		"IrodsAdminUser: rods\n" +
		"IrodsAuthScheme: native\n" +
		"IrodsNegotiationPolicy: native\n" +
		"PublicURL: http://trimmed.example\n" +
		"RestLogLevel: info\n"
	configPath := writeTestFile(t, dir, "custom-rest-config.yaml", configBody)

	t.Setenv(ConfigFileEnvVar, "  "+configPath+"  ")

	cfg, err := ReadRestConfig(" rest-config ", " yaml ", []string{"  " + dir + "  ", "   "})
	if err != nil {
		t.Fatalf("error reading config with whitespace-padded inputs: %v", err)
	}

	if cfg.PublicURL != "http://trimmed.example" {
		t.Fatalf("expected PublicURL from trimmed %s override, got %q", ConfigFileEnvVar, cfg.PublicURL)
	}

	if cfg.IrodsHost != "trimmed-host" {
		t.Fatalf("expected IrodsHost from trimmed %s override, got %q", ConfigFileEnvVar, cfg.IrodsHost)
	}
}
