package testconfig

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/michael-conway/irods-go-rest/internal/config"
)

const DefaultKeycloakEnvPath = "deployments/docker-test-framework/5-0/keycloak.env"
const keycloakRealmFileName = "realm-drs.json"

func ResolveTestSupportPath(repoRoot string, path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}

	if filepath.IsAbs(path) {
		return path
	}

	return filepath.Join(repoRoot, path)
}

func ApplyKeycloakEnvDefaults(repoRoot string, cfg *config.RestConfig, keycloakEnvPath string) error {
	if cfg == nil {
		return nil
	}

	keycloakEnvPath = strings.TrimSpace(keycloakEnvPath)
	if keycloakEnvPath == "" {
		keycloakEnvPath = DefaultKeycloakEnvPath
	}

	resolvedEnvPath := ResolveTestSupportPath(repoRoot, keycloakEnvPath)
	values, err := readKeycloakEnv(resolvedEnvPath)
	if err != nil {
		return err
	}

	if cfg.OidcUrl == "" {
		if hostname := strings.TrimSpace(values["KC_HOSTNAME"]); hostname != "" {
			cfg.OidcUrl = "https://" + hostname + ":8443"
		}
	}

	if cfg.OidcClientId == "" {
		cfg.OidcClientId = strings.TrimSpace(values["IRODS_REST_WEB_CLIENT_ID"])
	}

	if cfg.OidcClientSecret == "" {
		cfg.OidcClientSecret = strings.TrimSpace(values["IRODS_REST_WEB_CLIENT_SECRET"])
	}

	if cfg.OidcRealm == "" {
		realmFilePath := filepath.Join(filepath.Dir(resolvedEnvPath), keycloakRealmFileName)
		realm, err := readRealmName(realmFilePath)
		if err != nil {
			return err
		}
		cfg.OidcRealm = realm
	}

	return nil
}

func readKeycloakEnv(path string) (map[string]string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read keycloak env file %q: %w", path, err)
	}

	values := map[string]string{}
	for _, rawLine := range strings.Split(string(content), "\n") {
		line := strings.TrimSpace(rawLine)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		key, value, found := strings.Cut(line, "=")
		if !found {
			continue
		}

		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" {
			continue
		}

		values[key] = resolveShellStyleReference(value)
	}

	return values, nil
}

func resolveShellStyleReference(value string) string {
	value = strings.TrimSpace(value)
	if strings.HasPrefix(value, "${") && strings.HasSuffix(value, "}") && len(value) > 3 {
		return strings.TrimSpace(os.Getenv(value[2 : len(value)-1]))
	}

	return value
}

func readRealmName(path string) (string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read keycloak realm file %q: %w", path, err)
	}

	payload := struct {
		Realm string `json:"realm"`
	}{}
	if err := json.Unmarshal(content, &payload); err != nil {
		return "", fmt.Errorf("parse keycloak realm file %q: %w", path, err)
	}

	if strings.TrimSpace(payload.Realm) == "" {
		return "", fmt.Errorf("keycloak realm file %q does not define a realm", path)
	}

	return strings.TrimSpace(payload.Realm), nil
}
