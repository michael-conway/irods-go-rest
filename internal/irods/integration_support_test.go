//go:build integration
// +build integration

package irods

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	irodsfs "github.com/cyverse/go-irodsclient/fs"
	irodstypes "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/michael-conway/irods-go-rest/internal/config"
)

const integrationConfigFileEnvVar = "GOREST_E2E_CONFIG_FILE"

var (
	integrationConfigOnce  sync.Once
	integrationConfigValue *config.RestConfig
	integrationConfigErr   error
)

func newIntegrationCatalogService(t *testing.T) CatalogService {
	t.Helper()

	return NewCatalogService(integrationCatalogConfig(t))
}

func integrationCatalogConfig(t *testing.T) config.RestConfig {
	t.Helper()

	cfg := requireIntegrationRestConfig(t)
	requireNonEmptyIntegrationValue(t, "IrodsHost", cfg.IrodsHost)
	if cfg.IrodsPort <= 0 {
		t.Fatalf("integration tests require IrodsPort in %s", integrationConfigFileEnvVar)
	}
	requireNonEmptyIntegrationValue(t, "IrodsZone", cfg.IrodsZone)
	requireNonEmptyIntegrationValue(t, "IrodsAdminUser", cfg.IrodsAdminUser)
	requireNonEmptyIntegrationValue(t, "IrodsAdminPassword", cfg.IrodsAdminPassword)
	requireNonEmptyIntegrationValue(t, "IrodsAuthScheme", cfg.IrodsAuthScheme)

	return *cfg
}

func integrationBasicRequestContext(t *testing.T) *RequestContext {
	t.Helper()

	return &RequestContext{
		AuthScheme:    "basic",
		Username:      integrationBasicUsername(t),
		BasicPassword: integrationBasicPassword(t),
	}
}

func integrationCatalogRequestContext(t *testing.T) *RequestContext {
	t.Helper()

	if integrationUsesProxyUser(t) {
		return integrationBearerRequestContext(t)
	}

	return integrationBasicRequestContext(t)
}

func integrationBearerRequestContext(t *testing.T) *RequestContext {
	t.Helper()

	return &RequestContext{
		AuthScheme: "bearer",
		Username:   integrationBasicUsername(t),
	}
}

func newIntegrationIRODSFilesystem(t *testing.T) *irodsfs.FileSystem {
	t.Helper()

	cfg := integrationCatalogConfig(t)
	requestAuthScheme := cfg.RequestAuthScheme()
	adminAuthScheme := cfg.AdminAuthScheme()
	targetUser := integrationBasicUsername(t)
	defaultResource := integrationIRODSDefaultResource(t)
	var (
		account *irodstypes.IRODSAccount
		err     error
	)

	if integrationUsesProxyUser(t) {
		account, err = irodstypes.CreateIRODSProxyAccount(
			integrationIRODSHost(t),
			integrationIRODSPort(t),
			targetUser,
			integrationIRODSZone(t),
			integrationIRODSUser(t),
			integrationIRODSZone(t),
			adminAuthScheme,
			integrationIRODSPassword(t),
			defaultResource,
		)
	} else {
		account, err = irodstypes.CreateIRODSAccount(
			integrationIRODSHost(t),
			integrationIRODSPort(t),
			targetUser,
			integrationIRODSZone(t),
			requestAuthScheme,
			integrationBasicPassword(t),
			defaultResource,
		)
	}

	if err != nil {
		t.Fatalf("create iRODS account: %v", err)
	}
	cfg.ApplyIRODSConnectionConfig(account)

	filesystem, err := irodsfs.NewFileSystemWithDefault(account, "irods-go-rest-integration-test")
	if err != nil {
		t.Fatalf("connect to iRODS. This test requires a reachable iRODS test grid; prefer the backend services from irods-grid-stack: %v", err)
	}

	return filesystem
}

func integrationBasicUsername(t *testing.T) string {
	t.Helper()

	if cfg := requireIntegrationRestConfig(t); strings.TrimSpace(cfg.IrodsPrimaryTestUser) != "" {
		return strings.TrimSpace(cfg.IrodsPrimaryTestUser)
	}

	t.Fatalf("integration tests require IrodsPrimaryTestUser in %s", integrationConfigFileEnvVar)
	return ""
}

func integrationBasicPassword(t *testing.T) string {
	t.Helper()

	if cfg := requireIntegrationRestConfig(t); strings.TrimSpace(cfg.IrodsPrimaryTestPassword) != "" {
		return strings.TrimSpace(cfg.IrodsPrimaryTestPassword)
	}

	t.Fatalf("integration tests require IrodsPrimaryTestPassword in %s", integrationConfigFileEnvVar)
	return ""
}

func integrationIRODSHost(t *testing.T) string {
	t.Helper()

	if cfg := requireIntegrationRestConfig(t); strings.TrimSpace(cfg.IrodsHost) != "" {
		return strings.TrimSpace(cfg.IrodsHost)
	}

	t.Fatalf("integration tests require IrodsHost in %s", integrationConfigFileEnvVar)
	return ""
}

func integrationIRODSPort(t *testing.T) int {
	t.Helper()

	if cfg := requireIntegrationRestConfig(t); cfg.IrodsPort > 0 {
		return cfg.IrodsPort
	}

	t.Fatalf("integration tests require IrodsPort in %s", integrationConfigFileEnvVar)
	return 0
}

func integrationIRODSZone(t *testing.T) string {
	t.Helper()

	if cfg := requireIntegrationRestConfig(t); strings.TrimSpace(cfg.IrodsZone) != "" {
		return strings.TrimSpace(cfg.IrodsZone)
	}

	t.Fatalf("integration tests require IrodsZone in %s", integrationConfigFileEnvVar)
	return ""
}

func integrationIRODSAuthScheme(t *testing.T) string {
	t.Helper()

	if cfg := requireIntegrationRestConfig(t); strings.TrimSpace(cfg.IrodsAuthScheme) != "" {
		return strings.TrimSpace(cfg.IrodsAuthScheme)
	}

	t.Fatalf("integration tests require IrodsAuthScheme in %s", integrationConfigFileEnvVar)
	return ""
}

func integrationIRODSDefaultResource(t *testing.T) string {
	t.Helper()

	if cfg := requireIntegrationRestConfig(t); strings.TrimSpace(cfg.IrodsDefaultResource) != "" {
		return strings.TrimSpace(cfg.IrodsDefaultResource)
	}

	return ""
}

func integrationUsesProxyUser(t *testing.T) bool {
	t.Helper()

	return integrationIRODSUser(t) != integrationBasicUsername(t)
}

func integrationIRODSUser(t *testing.T) string {
	t.Helper()

	if cfg := requireIntegrationRestConfig(t); strings.TrimSpace(cfg.IrodsAdminUser) != "" {
		return strings.TrimSpace(cfg.IrodsAdminUser)
	}

	t.Fatalf("integration tests require IrodsAdminUser in %s", integrationConfigFileEnvVar)
	return ""
}

func integrationIRODSPassword(t *testing.T) string {
	t.Helper()

	if cfg := requireIntegrationRestConfig(t); strings.TrimSpace(cfg.IrodsAdminPassword) != "" {
		return strings.TrimSpace(cfg.IrodsAdminPassword)
	}

	t.Fatalf("integration tests require IrodsAdminPassword in %s", integrationConfigFileEnvVar)
	return ""
}

func requireIntegrationRestConfig(t *testing.T) *config.RestConfig {
	t.Helper()

	cfg := optionalIntegrationRestConfig(t)
	if cfg == nil {
		t.Fatalf("integration tests require %s to point at the shared E2E config file", integrationConfigFileEnvVar)
	}

	return cfg
}

func optionalIntegrationRestConfig(t *testing.T) *config.RestConfig {
	integrationConfigOnce.Do(func() {
		loadIntegrationConfigs()
	})

	if integrationConfigErr != nil && t != nil {
		t.Fatalf("%v", integrationConfigErr)
	}

	return integrationConfigValue
}

func requireNonEmptyIntegrationValue(t *testing.T, field string, value string) {
	t.Helper()

	if strings.TrimSpace(value) == "" {
		t.Fatalf("integration tests require %s in %s", field, integrationConfigFileEnvVar)
	}
}

func loadIntegrationConfigs() {
	configFile := strings.TrimSpace(os.Getenv(integrationConfigFileEnvVar))
	if configFile == "" {
		return
	}

	resolvedPath, err := resolveIntegrationConfigPath(configFile)
	if err != nil {
		integrationConfigErr = err
		return
	}

	originalConfigFile := os.Getenv(config.ConfigFileEnvVar)
	_ = os.Setenv(config.ConfigFileEnvVar, resolvedPath)
	defer func() {
		_ = os.Setenv(config.ConfigFileEnvVar, originalConfigFile)
	}()

	cfg, err := config.ReadRestConfig("", "", nil)
	if err != nil {
		integrationConfigErr = fmt.Errorf("read integration rest config from %s=%q: %w", integrationConfigFileEnvVar, resolvedPath, err)
		return
	}

	integrationConfigValue = cfg
}

func resolveIntegrationConfigPath(configFile string) (string, error) {
	configFile = strings.TrimSpace(configFile)
	if configFile == "" {
		return "", fmt.Errorf("empty config file path")
	}

	if filepath.IsAbs(configFile) {
		return configFile, nil
	}

	_, _, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("resolve relative %s path %q: runtime caller unavailable", config.ConfigFileEnvVar, configFile)
	}

	repoRoot, err := integrationRepoRoot()
	if err != nil {
		return "", err
	}
	return filepath.Join(repoRoot, configFile), nil
}

func integrationRepoRoot() (string, error) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("resolve relative %s path: runtime caller unavailable", integrationConfigFileEnvVar)
	}

	irodsDir := filepath.Dir(filename)
	return filepath.Dir(filepath.Dir(irodsDir)), nil
}
