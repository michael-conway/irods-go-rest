//go:build e2e
// +build e2e

package e2e

import (
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/michael-conway/irods-go-rest/internal/config"
	"github.com/spf13/viper"
)

var (
	e2eConfigOnce  sync.Once
	e2eConfigValue *config.RestConfig
	e2eFileConfig  *e2eTestConfig
	e2eConfigErr   error
)

const e2eConfigFileEnvVar = "GOREST_E2E_CONFIG_FILE"

type e2eTestConfig struct {
	E2E struct {
		BaseURL       string
		BasicUsername string
		BasicPassword string
		IRODSUser     string
		IRODSPassword string
		SkipTLSVerify bool
		BearerToken   string
	}
}

func requireE2EBaseURL(t *testing.T) string {
	t.Helper()

	baseURL := strings.TrimSpace(os.Getenv("GOREST_E2E_BASE_URL"))
	if baseURL != "" {
		return baseURL
	}

	if cfg := optionalE2EFileConfig(t); cfg != nil && strings.TrimSpace(cfg.E2E.BaseURL) != "" {
		return strings.TrimSpace(cfg.E2E.BaseURL)
	}

	cfg := optionalE2ERestConfig(t)
	if cfg != nil && strings.TrimSpace(cfg.PublicURL) != "" {
		return strings.TrimSpace(cfg.PublicURL)
	}

	t.Fatalf("e2e tests require E2E.BaseURL, PublicURL, or GOREST_E2E_BASE_URL with %s set", e2eConfigFileEnvVar)
	return ""
}

func requireE2EBearerToken(t *testing.T) string {
	t.Helper()

	token := strings.TrimSpace(os.Getenv("DRS_TEST_BEARER_TOKEN"))
	if token == "" {
		if cfg := optionalE2EFileConfig(t); cfg != nil {
			token = strings.TrimSpace(cfg.E2E.BearerToken)
		}
	}
	if token == "" {
		t.Skip("DRS_TEST_BEARER_TOKEN is not set")
	}

	return token
}

func e2eBasicUsername(t *testing.T) string {
	if t != nil {
		t.Helper()
	}

	if value := strings.TrimSpace(os.Getenv("GOREST_E2E_BASIC_USERNAME")); value != "" {
		return value
	}

	if cfg := optionalE2EFileConfig(nil); cfg != nil && strings.TrimSpace(cfg.E2E.BasicUsername) != "" {
		return strings.TrimSpace(cfg.E2E.BasicUsername)
	}

	t.Fatalf("e2e tests require E2E.BasicUsername in %s or GOREST_E2E_BASIC_USERNAME", e2eConfigFileEnvVar)
	return ""
}

func e2eBasicPassword(t *testing.T) string {
	if t != nil {
		t.Helper()
	}

	if value := strings.TrimSpace(os.Getenv("GOREST_E2E_BASIC_PASSWORD")); value != "" {
		return value
	}

	if cfg := optionalE2EFileConfig(nil); cfg != nil && strings.TrimSpace(cfg.E2E.BasicPassword) != "" {
		return strings.TrimSpace(cfg.E2E.BasicPassword)
	}

	t.Fatalf("e2e tests require E2E.BasicPassword in %s or GOREST_E2E_BASIC_PASSWORD", e2eConfigFileEnvVar)
	return ""
}

func e2eIRODSHost(t *testing.T) string {
	if t != nil {
		t.Helper()
	}

	if value := strings.TrimSpace(os.Getenv("GOREST_E2E_IRODS_HOST")); value != "" {
		return value
	}

	if cfg := optionalE2ERestConfig(t); cfg != nil && strings.TrimSpace(cfg.IrodsHost) != "" {
		return strings.TrimSpace(cfg.IrodsHost)
	}

	t.Fatalf("e2e tests require IrodsHost in %s", e2eConfigFileEnvVar)
	return ""
}

func e2eIRODSPort(t *testing.T) int {
	t.Helper()

	raw := strings.TrimSpace(os.Getenv("GOREST_E2E_IRODS_PORT"))
	if raw == "" {
		if cfg := optionalE2ERestConfig(t); cfg != nil && cfg.IrodsPort > 0 {
			return cfg.IrodsPort
		}
		t.Fatalf("e2e tests require IrodsPort in %s", e2eConfigFileEnvVar)
	}

	port, err := strconv.Atoi(raw)
	if err != nil {
		t.Fatalf("invalid GOREST_E2E_IRODS_PORT %q: %v", raw, err)
	}

	return port
}

func e2eIRODSZone(t *testing.T) string {
	if t != nil {
		t.Helper()
	}

	if value := strings.TrimSpace(os.Getenv("GOREST_E2E_IRODS_ZONE")); value != "" {
		return value
	}

	if cfg := optionalE2ERestConfig(t); cfg != nil && strings.TrimSpace(cfg.IrodsZone) != "" {
		return strings.TrimSpace(cfg.IrodsZone)
	}

	t.Fatalf("e2e tests require IrodsZone in %s", e2eConfigFileEnvVar)
	return ""
}

func e2eIRODSAuthScheme(t *testing.T) string {
	if t != nil {
		t.Helper()
	}

	if cfg := optionalE2ERestConfig(t); cfg != nil && strings.TrimSpace(cfg.IrodsAuthScheme) != "" {
		return strings.TrimSpace(cfg.IrodsAuthScheme)
	}

	t.Fatalf("e2e tests require IrodsAuthScheme in %s", e2eConfigFileEnvVar)
	return ""
}

func e2eIRODSDefaultResource(t *testing.T) string {
	if t != nil {
		t.Helper()
	}

	if cfg := optionalE2ERestConfig(t); cfg != nil && strings.TrimSpace(cfg.IrodsDefaultResource) != "" {
		return strings.TrimSpace(cfg.IrodsDefaultResource)
	}

	return ""
}

func e2eIRODSUser(t *testing.T) string {
	if t != nil {
		t.Helper()
	}

	if value := strings.TrimSpace(os.Getenv("GOREST_E2E_IRODS_USER")); value != "" {
		return value
	}

	if cfg := optionalE2EFileConfig(nil); cfg != nil && strings.TrimSpace(cfg.E2E.IRODSUser) != "" {
		return strings.TrimSpace(cfg.E2E.IRODSUser)
	}

	return e2eBasicUsername(t)
}

func e2eIRODSPassword(t *testing.T) string {
	if t != nil {
		t.Helper()
	}

	if value := strings.TrimSpace(os.Getenv("GOREST_E2E_IRODS_PASSWORD")); value != "" {
		return value
	}

	if cfg := optionalE2EFileConfig(nil); cfg != nil && strings.TrimSpace(cfg.E2E.IRODSPassword) != "" {
		return strings.TrimSpace(cfg.E2E.IRODSPassword)
	}

	if e2eUsesProxyUser(t) {
		t.Fatalf("e2e tests require E2E.IRODSPassword in %s or GOREST_E2E_IRODS_PASSWORD when using a proxy uploader", e2eConfigFileEnvVar)
	}

	return e2eBasicPassword(t)
}

func newE2EHTTPClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	skipTLSVerify := strings.EqualFold(strings.TrimSpace(os.Getenv("GOREST_E2E_SKIP_TLS_VERIFY")), "true")
	if !skipTLSVerify {
		if cfg := optionalE2EFileConfig(nil); cfg != nil {
			skipTLSVerify = cfg.E2E.SkipTLSVerify
		}
	}
	if skipTLSVerify {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}

	return &http.Client{
		Transport: transport,
		Timeout:   30 * time.Second,
	}
}

func newE2ERequest(t *testing.T, method string, url string, body io.Reader) *http.Request {
	t.Helper()

	req, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}

	return req
}

func setBasicAuth(req *http.Request) {
	credentials := e2eBasicUsername(nil) + ":" + e2eBasicPassword(nil)
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(credentials)))
}

func setBearerAuth(req *http.Request, token string) {
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(token))
}

func optionalE2ERestConfig(t *testing.T) *config.RestConfig {
	e2eConfigOnce.Do(func() {
		loadE2EConfigs()
	})

	if e2eConfigErr != nil && t != nil {
		t.Fatalf("%v", e2eConfigErr)
	}

	return e2eConfigValue
}

func optionalE2EFileConfig(t *testing.T) *e2eTestConfig {
	e2eConfigOnce.Do(func() {
		loadE2EConfigs()
	})

	if e2eConfigErr != nil && t != nil {
		t.Fatalf("%v", e2eConfigErr)
	}

	return e2eFileConfig
}

func loadE2EConfigs() {
	configFile := strings.TrimSpace(os.Getenv(e2eConfigFileEnvVar))
	if configFile == "" {
		return
	}

	resolvedPath, err := resolveE2EConfigPath(configFile)
	if err != nil {
		e2eConfigErr = err
		return
	}

	fileCfg, err := readE2ETestConfig(resolvedPath)
	if err != nil {
		e2eConfigErr = fmt.Errorf("read e2e config from %s=%q: %w", e2eConfigFileEnvVar, resolvedPath, err)
		return
	}
	e2eFileConfig = fileCfg

	originalConfigFile := os.Getenv(config.ConfigFileEnvVar)
	_ = os.Setenv(config.ConfigFileEnvVar, resolvedPath)
	defer func() {
		_ = os.Setenv(config.ConfigFileEnvVar, originalConfigFile)
	}()

	cfg, err := config.ReadRestConfig("", "", nil)
	if err != nil {
		e2eConfigErr = fmt.Errorf("read e2e rest config from %s=%q: %w", e2eConfigFileEnvVar, resolvedPath, err)
		return
	}

	e2eConfigValue = cfg
}

func readE2ETestConfig(configFile string) (*e2eTestConfig, error) {
	v := viper.New()
	v.SetConfigFile(configFile)
	if err := v.ReadInConfig(); err != nil {
		return nil, err
	}

	cfg := &e2eTestConfig{}
	if err := v.Unmarshal(cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

func resolveE2EConfigPath(configFile string) (string, error) {
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

	repoRoot, err := e2eRepoRoot()
	if err != nil {
		return "", err
	}
	return filepath.Join(repoRoot, configFile), nil
}

func e2eUsesProxyUser(t *testing.T) bool {
	if t != nil {
		t.Helper()
	}

	return e2eIRODSUser(t) != e2eBasicUsername(t)
}

func e2eRepoRoot() (string, error) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("resolve relative %s path: runtime caller unavailable", e2eConfigFileEnvVar)
	}

	e2eDir := filepath.Dir(filename)
	return filepath.Dir(e2eDir), nil
}
