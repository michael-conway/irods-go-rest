package config

import (
	"os"
	"path/filepath"
	"testing"

	irodstypes "github.com/cyverse/go-irodsclient/irods/types"
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
	t.Setenv("GOREST_RESOURCE_AFFINITY", "demoResc, edgeResc ,  archiveResc  ")
	t.Setenv("GOREST_S3_BUCKET_MAPPING_FILE", "/tmp/s3-buckets.json")
	t.Setenv("GOREST_REPLICA_TRIM_MIN_COPIES", "4")
	t.Setenv("GOREST_REPLICA_TRIM_MIN_AGE_MINUTES", "12")
	t.Setenv("IRODS_REST_ADDR", ":18080")
	t.Setenv("GOREST_IRODS_ADMIN_LOGIN_TYPE", "native")

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
	if len(cfg.ResourceAffinity) != 3 || cfg.ResourceAffinity[0] != "demoResc" || cfg.ResourceAffinity[1] != "edgeResc" || cfg.ResourceAffinity[2] != "archiveResc" {
		t.Fatalf("expected trimmed ResourceAffinity from env override, got %+v", cfg.ResourceAffinity)
	}
	if cfg.S3BucketMappingFile != "/tmp/s3-buckets.json" {
		t.Fatalf("expected S3BucketMappingFile from env override, got %q", cfg.S3BucketMappingFile)
	}
	if cfg.ReplicaTrimMinCopies != 4 {
		t.Fatalf("expected ReplicaTrimMinCopies from env override, got %d", cfg.ReplicaTrimMinCopies)
	}
	if cfg.ReplicaTrimMinAgeMinutes != 12 {
		t.Fatalf("expected ReplicaTrimMinAgeMinutes from env override, got %d", cfg.ReplicaTrimMinAgeMinutes)
	}
	if cfg.ListenAddr != ":18080" {
		t.Fatalf("expected ListenAddr from IRODS_REST_ADDR override, got %q", cfg.ListenAddr)
	}
	if cfg.IrodsAdminLoginType != "native" {
		t.Fatalf("expected IrodsAdminLoginType from env override, got %q", cfg.IrodsAdminLoginType)
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
		"IrodsNegotiationPolicy: CS_NEG_DONT_CARE\n" +
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

func TestReadRestConfigResourceAffinityYAMLList(t *testing.T) {
	dir := t.TempDir()
	configBody := "" +
		"IrodsHost: localhost\n" +
		"IrodsPort: 1247\n" +
		"IrodsZone: tempZone\n" +
		"IrodsAdminUser: rods\n" +
		"IrodsAuthScheme: native\n" +
		"IrodsNegotiationPolicy: CS_NEG_DONT_CARE\n" +
		"ResourceAffinity:\n" +
		"  - demoResc\n" +
		"  - edgeResc\n" +
		"  - archiveResc\n"

	writeTestFile(t, dir, "rest-config.yaml", configBody)

	cfg, err := ReadRestConfig("rest-config", "yaml", []string{dir})
	if err != nil {
		t.Fatalf("error reading config: %v", err)
	}

	if len(cfg.ResourceAffinity) != 3 || cfg.ResourceAffinity[0] != "demoResc" || cfg.ResourceAffinity[1] != "edgeResc" || cfg.ResourceAffinity[2] != "archiveResc" {
		t.Fatalf("expected ResourceAffinity list from YAML, got %+v", cfg.ResourceAffinity)
	}
}

func TestReadRestConfigIRODSSSLConfigYAML(t *testing.T) {
	dir := t.TempDir()
	configBody := "" +
		"IrodsHost: localhost\n" +
		"IrodsPort: 1247\n" +
		"IrodsZone: tempZone\n" +
		"IrodsAdminUser: rods\n" +
		"IrodsAuthScheme: native\n" +
		"IrodsNegotiationPolicy: CS_NEG_REQUIRE\n" +
		"IrodsSSLConfig:\n" +
		"  CACertificateFile: /etc/irods/ca.pem\n" +
		"  CACertificatePath: /etc/irods/certs\n" +
		"  EncryptionKeySize: 32\n" +
		"  EncryptionAlgorithm: AES-256-CBC\n" +
		"  EncryptionSaltSize: 8\n" +
		"  EncryptionNumHashRounds: 16\n" +
		"  VerifyServer: hostname\n" +
		"  DHParamsFile: /etc/irods/dhparams.pem\n" +
		"  ServerName: irods.example.org\n"

	writeTestFile(t, dir, "rest-config.yaml", configBody)

	cfg, err := ReadRestConfig("rest-config", "yaml", []string{dir})
	if err != nil {
		t.Fatalf("error reading config: %v", err)
	}

	if cfg.IrodsSSLConfig.CACertificateFile != "/etc/irods/ca.pem" {
		t.Fatalf("expected CA certificate file from YAML, got %q", cfg.IrodsSSLConfig.CACertificateFile)
	}
	if cfg.IrodsSSLConfig.ServerName != "irods.example.org" {
		t.Fatalf("expected SSL server name from YAML, got %q", cfg.IrodsSSLConfig.ServerName)
	}

	account := cfg.ToIrodsAccount()
	if !account.ClientServerNegotiation {
		t.Fatal("expected client-server negotiation for SSL policy")
	}
	if account.CSNegotiationPolicy != irodstypes.CSNegotiationPolicyRequestSSL {
		t.Fatalf("expected SSL negotiation policy, got %q", account.CSNegotiationPolicy)
	}
	if account.SSLConfiguration == nil {
		t.Fatal("expected SSL configuration on account")
	}
	if account.SSLConfiguration.VerifyServer != irodstypes.SSLVerifyServerHostname {
		t.Fatalf("expected hostname verification, got %q", account.SSLConfiguration.VerifyServer)
	}
	if account.SSLConfiguration.ServerName != "irods.example.org" {
		t.Fatalf("expected SSL server name on account, got %q", account.SSLConfiguration.ServerName)
	}
}

func TestAdminAuthSchemeFallsBackToRequestAuthScheme(t *testing.T) {
	cfg := RestConfig{IrodsAuthScheme: "pam"}

	if got := cfg.AdminAuthScheme(); got != irodstypes.AuthSchemePAM {
		t.Fatalf("expected admin auth scheme to fall back to request auth scheme, got %q", got)
	}
}

func TestAdminAuthSchemeUsesAdminLoginType(t *testing.T) {
	cfg := RestConfig{
		IrodsAdminLoginType: "native",
		IrodsAuthScheme:     "pam",
	}

	if got := cfg.AdminAuthScheme(); got != irodstypes.AuthSchemeNative {
		t.Fatalf("expected admin auth scheme from IrodsAdminLoginType, got %q", got)
	}
	if got := cfg.RequestAuthScheme(); got != irodstypes.AuthSchemePAM {
		t.Fatalf("expected request auth scheme from IrodsAuthScheme, got %q", got)
	}
}

func TestReadRestConfigIRODSSSLConfigEnvOverride(t *testing.T) {
	dir := t.TempDir()
	configBody := "" +
		"IrodsHost: localhost\n" +
		"IrodsPort: 1247\n" +
		"IrodsZone: tempZone\n" +
		"IrodsAdminUser: rods\n" +
		"IrodsAuthScheme: native\n" +
		"IrodsNegotiationPolicy: CS_NEG_DONT_CARE\n" +
		"RestLogLevel: info\n"

	writeTestFile(t, dir, "rest-config.yaml", configBody)

	t.Setenv("GOREST_IRODS_SSL_CA_CERTIFICATE_FILE", "/env/ca.pem")
	t.Setenv("GOREST_IRODS_SSL_VERIFY_SERVER", "none")
	t.Setenv("GOREST_IRODS_SSL_SERVER_NAME", "env-irods.example.org")
	t.Setenv("GOREST_IRODS_ENCRYPTION_KEY_SIZE", "64")

	cfg, err := ReadRestConfig("rest-config", "yaml", []string{dir})
	if err != nil {
		t.Fatalf("error reading config: %v", err)
	}

	if cfg.IrodsSSLConfig.CACertificateFile != "/env/ca.pem" {
		t.Fatalf("expected SSL CA file from env override, got %q", cfg.IrodsSSLConfig.CACertificateFile)
	}
	if cfg.IrodsSSLConfig.VerifyServer != "none" {
		t.Fatalf("expected SSL verify server from env override, got %q", cfg.IrodsSSLConfig.VerifyServer)
	}
	if cfg.IrodsSSLConfig.EncryptionKeySize != 64 {
		t.Fatalf("expected SSL encryption key size from env override, got %d", cfg.IrodsSSLConfig.EncryptionKeySize)
	}

	sslConfig := cfg.ToIRODSSSLConfig()
	if sslConfig.VerifyServer != irodstypes.SSLVerifyServerNone {
		t.Fatalf("expected no server verification, got %q", sslConfig.VerifyServer)
	}
	if sslConfig.ServerName != "env-irods.example.org" {
		t.Fatalf("expected SSL server name from env override, got %q", sslConfig.ServerName)
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
		"IrodsNegotiationPolicy: CS_NEG_DONT_CARE\n" +
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
		"IrodsNegotiationPolicy: CS_NEG_DONT_CARE\n" +
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

func TestReadRestConfigInvalidNegotiationPolicyDefaultsToDontCare(t *testing.T) {
	dir := t.TempDir()
	configBody := "" +
		"IrodsHost: localhost\n" +
		"IrodsPort: 1247\n" +
		"IrodsZone: tempZone\n" +
		"IrodsAdminUser: rods\n" +
		"IrodsAuthScheme: native\n" +
		"IrodsNegotiationPolicy: INVALID_NEGOTIATION_POLICY\n" +
		"RestLogLevel: info\n"
	writeTestFile(t, dir, "rest-config.yaml", configBody)

	cfg, err := ReadRestConfig("rest-config", "yaml", []string{dir})
	if err != nil {
		t.Fatalf("error reading config: %v", err)
	}

	if cfg.IrodsNegotiationPolicy != "CS_NEG_DONT_CARE" {
		t.Fatalf("expected invalid negotiation policy to normalize to CS_NEG_DONT_CARE, got %q", cfg.IrodsNegotiationPolicy)
	}
}

func TestReadRestConfigReplicaTrimDefaults(t *testing.T) {
	dir := t.TempDir()
	configBody := "" +
		"IrodsHost: localhost\n" +
		"IrodsPort: 1247\n" +
		"IrodsZone: tempZone\n" +
		"IrodsAdminUser: rods\n" +
		"IrodsAuthScheme: native\n" +
		"IrodsNegotiationPolicy: CS_NEG_DONT_CARE\n" +
		"RestLogLevel: info\n"
	writeTestFile(t, dir, "rest-config.yaml", configBody)

	cfg, err := ReadRestConfig("rest-config", "yaml", []string{dir})
	if err != nil {
		t.Fatalf("error reading config: %v", err)
	}

	if cfg.ReplicaTrimMinCopies != 1 {
		t.Fatalf("expected ReplicaTrimMinCopies default 1, got %d", cfg.ReplicaTrimMinCopies)
	}
	if cfg.ReplicaTrimMinAgeMinutes != 0 {
		t.Fatalf("expected ReplicaTrimMinAgeMinutes default 0, got %d", cfg.ReplicaTrimMinAgeMinutes)
	}
}
