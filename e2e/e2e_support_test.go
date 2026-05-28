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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/michael-conway/irods-go-rest/internal/config"
)

var (
	e2eConfigOnce  sync.Once
	e2eConfigValue *config.RestConfig
	e2eConfigErr   error
)

const e2eConfigFileEnvVar = "GOREST_E2E_CONFIG_FILE"

func requireE2EBaseURL(t *testing.T) string {
	t.Helper()

	cfg := optionalE2ERestConfig(t)
	if cfg != nil && strings.TrimSpace(cfg.PublicURL) != "" {
		return strings.TrimSpace(cfg.PublicURL)
	}

	t.Fatalf("e2e tests require PublicURL in %s", e2eConfigFileEnvVar)
	return ""
}

func requireE2EBearerToken(t *testing.T) string {
	t.Helper()

	token := ""
	if cfg := optionalE2ERestConfig(t); cfg != nil {
		token = strings.TrimSpace(cfg.TestBearerToken)
	}
	if token == "" {
		t.Skip("TestBearerToken is not set in GOREST_E2E_CONFIG_FILE")
	}

	return token
}

func e2eBasicUsername(t *testing.T) string {
	if t != nil {
		t.Helper()
	}

	if cfg := optionalE2ERestConfig(t); cfg != nil && strings.TrimSpace(cfg.IrodsPrimaryTestUser) != "" {
		return strings.TrimSpace(cfg.IrodsPrimaryTestUser)
	}

	t.Fatalf("e2e tests require IrodsPrimaryTestUser in %s", e2eConfigFileEnvVar)
	return ""
}

func e2eBasicPassword(t *testing.T) string {
	if t != nil {
		t.Helper()
	}

	if cfg := optionalE2ERestConfig(t); cfg != nil && strings.TrimSpace(cfg.IrodsPrimaryTestPassword) != "" {
		return strings.TrimSpace(cfg.IrodsPrimaryTestPassword)
	}

	t.Fatalf("e2e tests require IrodsPrimaryTestPassword in %s", e2eConfigFileEnvVar)
	return ""
}

func e2eIRODSHost(t *testing.T) string {
	if t != nil {
		t.Helper()
	}

	if cfg := optionalE2ERestConfig(t); cfg != nil && strings.TrimSpace(cfg.IrodsHost) != "" {
		return strings.TrimSpace(cfg.IrodsHost)
	}

	t.Fatalf("e2e tests require IrodsHost in %s", e2eConfigFileEnvVar)
	return ""
}

func e2eIRODSPort(t *testing.T) int {
	t.Helper()

	if cfg := optionalE2ERestConfig(t); cfg != nil && cfg.IrodsPort > 0 {
		return cfg.IrodsPort
	}

	t.Fatalf("e2e tests require IrodsPort in %s", e2eConfigFileEnvVar)
	return 0
}

func e2eIRODSZone(t *testing.T) string {
	if t != nil {
		t.Helper()
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

func e2eS3APISupported(t *testing.T) bool {
	if t != nil {
		t.Helper()
	}

	if cfg := optionalE2ERestConfig(t); cfg != nil {
		return cfg.S3ApiSupported
	}

	return false
}

func e2eTestResource1(t *testing.T) string {
	if t != nil {
		t.Helper()
	}

	if cfg := optionalE2ERestConfig(t); cfg != nil && strings.TrimSpace(cfg.TestResource1) != "" {
		return strings.TrimSpace(cfg.TestResource1)
	}

	t.Fatalf("e2e tests require TestResource1 in %s", e2eConfigFileEnvVar)
	return ""
}

func e2eTestResource2(t *testing.T) string {
	if t != nil {
		t.Helper()
	}

	if cfg := optionalE2ERestConfig(t); cfg != nil && strings.TrimSpace(cfg.TestResource2) != "" {
		return strings.TrimSpace(cfg.TestResource2)
	}

	t.Fatalf("e2e tests require TestResource2 in %s", e2eConfigFileEnvVar)
	return ""
}

func e2eIRODSUser(t *testing.T) string {
	if t != nil {
		t.Helper()
	}

	if cfg := optionalE2ERestConfig(t); cfg != nil && strings.TrimSpace(cfg.IrodsAdminUser) != "" {
		return strings.TrimSpace(cfg.IrodsAdminUser)
	}

	t.Fatalf("e2e tests require IrodsAdminUser in %s", e2eConfigFileEnvVar)
	return ""
}

func e2eIRODSPassword(t *testing.T) string {
	if t != nil {
		t.Helper()
	}

	if cfg := optionalE2ERestConfig(t); cfg != nil && strings.TrimSpace(cfg.IrodsAdminPassword) != "" {
		return strings.TrimSpace(cfg.IrodsAdminPassword)
	}

	t.Fatalf("e2e tests require IrodsAdminPassword in %s", e2eConfigFileEnvVar)
	return ""
}

func newE2EHTTPClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	skipTLSVerify := false
	if cfg := optionalE2ERestConfig(nil); cfg != nil {
		skipTLSVerify = cfg.OidcInsecureSkipVerify
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

func sameHostOrLoopback(actual string, expected string) bool {
	normalize := func(host string) string {
		host = strings.TrimSpace(strings.ToLower(host))
		host = strings.TrimPrefix(host, "[")
		host = strings.TrimSuffix(host, "]")

		switch host {
		case "localhost", "127.0.0.1", "::1":
			return "loopback"
		default:
			return host
		}
	}

	return normalize(actual) == normalize(expected)
}

func setBasicAuthCredentials(req *http.Request, username string, password string) {
	credentials := strings.TrimSpace(username) + ":" + strings.TrimSpace(password)
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
