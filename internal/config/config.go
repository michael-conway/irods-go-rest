package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/rs/zerolog"
	"github.com/spf13/viper"
)

// RestConfig Provides configuration for drs behaviors
type RestConfig struct {
	PublicURL              string
	RestLogLevel           string //info, debug
	IrodsHost              string
	IrodsPort              int
	IrodsZone              string
	IrodsAdminUser         string
	IrodsAdminPassword     string
	IrodsAdminPasswordFile string
	IrodsAuthScheme        string
	IrodsNegotiationPolicy string
	IrodsDefaultResource   string
	OidcUrl                string
	OidcClientId           string
	OidcClientSecret       string
	OidcClientSecretFile   string
	OidcInsecureSkipVerify bool
	OidcRealm              string
	OidcScope              string
}

func (cfg *RestConfig) ToIrodsAccount() types.IRODSAccount {
	authScheme := types.GetAuthScheme(cfg.IrodsAuthScheme)

	negotiationPolicy := types.GetCSNegotiationPolicyRequest(cfg.IrodsNegotiationPolicy)
	negotiation := types.GetCSNegotiation(cfg.IrodsNegotiationPolicy)

	account := types.IRODSAccount{
		AuthenticationScheme:    authScheme,
		ClientServerNegotiation: negotiation.IsNegotiationRequired(),
		CSNegotiationPolicy:     negotiationPolicy,
		Host:                    cfg.IrodsHost,
		Port:                    cfg.IrodsPort,
		ClientUser:              cfg.IrodsAdminUser,
		ClientZone:              cfg.IrodsZone,
		ProxyUser:               cfg.IrodsAdminUser,
		ProxyZone:               cfg.IrodsZone,
		Password:                cfg.IrodsAdminPassword,
		DefaultResource:         cfg.IrodsDefaultResource,
	}

	account.FixAuthConfiguration()

	return account
}

const DefaultConfigName = "rest-config"
const DefaultConfigType = "yaml"
const ConfigFileEnvVar = "IRODS_REST_CONFIG_FILE"

func bindEnvVars(v *viper.Viper) error {
	envBindings := map[string][]string{
		"PublicURL":              {"GOREST_PUBLIC_URL", "GOREST_PUBLICURL"},
		"RestLogLevel":           {"GOREST_REST_LOG_LEVEL", "GOREST_RESTLOGLEVEL"},
		"IrodsHost":              {"GOREST_IRODS_HOST", "GOREST_IRODSHOST"},
		"IrodsPort":              {"GOREST_IRODS_PORT", "GOREST_IRODSPORT"},
		"IrodsZone":              {"GOREST_IRODS_ZONE", "GOREST_IRODSZONE"},
		"IrodsAdminUser":         {"GOREST_IRODS_ADMIN_USER", "GOREST_IRODSADMINUSER"},
		"IrodsAdminPassword":     {"GOREST_IRODS_ADMIN_PASSWORD", "GOREST_IRODSADMINPASSWORD"},
		"IrodsAdminPasswordFile": {"GOREST_IRODS_ADMIN_PASSWORD_FILE", "GOREST_IRODSADMINPASSWORDFILE"},
		"IrodsAuthScheme":        {"GOREST_IRODS_AUTH_SCHEME", "GOREST_IRODSAUTHSCHEME"},
		"IrodsNegotiationPolicy": {"GOREST_IRODS_NEGOTIATION_POLICY", "GOREST_IRODSNEGOTIATIONPOLICY"},
		"IrodsDefaultResource":   {"GOREST_IRODS_DEFAULT_RESOURCE", "GOREST_IRODSDEFAULTRESOURCE"},
		"OidcUrl":                {"GOREST_OIDC_URL", "GOREST_OIDCURL"},
		"OidcClientId":           {"GOREST_OIDC_CLIENT_ID", "GOREST_OIDCCLIENTID"},
		"OidcClientSecret":       {"GOREST_OIDC_CLIENT_SECRET", "GOREST_OIDCCLIENTSECRET"},
		"OidcClientSecretFile":   {"GOREST_OIDC_CLIENT_SECRET_FILE", "GOREST_OIDCCLIENTSECRETFILE"},
		"OidcInsecureSkipVerify": {"GOREST_OIDC_INSECURE_SKIP_VERIFY", "GOREST_OIDCINSECURESKIPVERIFY"},
		"OidcRealm":              {"GOREST_OIDC_REALM", "GOREST_OIDCREALM"},
		"OidcScope":              {"GOREST_OIDC_SCOPE", "GOREST_OIDCSCOPE"},
	}

	for key, envNames := range envBindings {
		bindingArgs := append([]string{key}, envNames...)
		if err := v.BindEnv(bindingArgs...); err != nil {
			return fmt.Errorf("failed to bind env for %s: %w", key, err)
		}
	}

	return nil
}

func resolveSecret(secret string, secretFile string, secretName string) (string, error) {
	if secret != "" {
		return secret, nil
	}

	if secretFile == "" {
		return "", nil
	}

	secretBytes, err := os.ReadFile(secretFile)
	if err != nil {
		return "", fmt.Errorf("failed to read %s file %q: %w", secretName, secretFile, err)
	}

	return strings.TrimSpace(string(secretBytes)), nil
}

// ReadRestConfig reads the configuration for REST behaviors in irods
// can take a number of paths that will be prefixed in the searh path, or defaults
// may be accepted, blank params for name and type default to irods-drs.yaml
func ReadRestConfig(configName string, configType string, configPaths []string) (*RestConfig, error) {
	v := viper.New()

	if configFilePath := os.Getenv(ConfigFileEnvVar); configFilePath != "" {
		v.SetConfigFile(configFilePath)
	} else {
		if configName == "" {
			v.SetConfigName(DefaultConfigName) // name of config file (without extension)
		} else {
			v.SetConfigName(configName)
		}

		if configType == "" {
			v.SetConfigType(DefaultConfigType) // REQUIRED if the config file does not have the extension in the name
		} else {
			v.SetConfigType(configType)
		}

		for _, path := range configPaths {
			v.AddConfigPath(path)
		}

		v.AddConfigPath("/etc/irods-ext/")      // path to look for the config file in
		v.AddConfigPath("$HOME/.irods-go-rest") // call multiple times to add many search paths
		v.AddConfigPath(".")                    // optionally look for config in the working directory
	}

	if err := bindEnvVars(v); err != nil {
		return nil, err
	}

	err := v.ReadInConfig() // Find and read the config file
	if err != nil {         // Handle errors reading the config file
		return nil, fmt.Errorf("fatal error config file: %w", err)
	}
	var C RestConfig

	err = v.Unmarshal(&C)
	if err != nil {
		return nil, fmt.Errorf("unable to decode into struct: %w", err)
	}

	C.IrodsAdminPassword, err = resolveSecret(C.IrodsAdminPassword, C.IrodsAdminPasswordFile, "iRODS admin password")
	if err != nil {
		return nil, err
	}

	C.OidcClientSecret, err = resolveSecret(C.OidcClientSecret, C.OidcClientSecretFile, "OIDC client secret")
	if err != nil {
		return nil, err
	}

	return &C, nil
}

func (d *RestConfig) InitializeLogging() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	switch d.RestLogLevel {
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "warn":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case "error":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	default:
		zerolog.SetGlobalLevel(zerolog.InfoLevel)

	}
}
