package config

import (
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"strings"

	"github.com/cyverse/go-irodsclient/irods/types"
	"github.com/go-viper/mapstructure/v2"
	"github.com/rs/zerolog"
	"github.com/spf13/viper"
)

// RestConfig Provides configuration for drs behaviors
type RestConfig struct {
	PublicURL                string
	ListenAddr               string
	RestLogLevel             string //info, debug
	IrodsHost                string
	IrodsPort                int
	IrodsZone                string
	IrodsAdminUser           string
	IrodsAdminPassword       string
	IrodsAdminPasswordFile   string
	IrodsAdminLoginType      string
	IrodsAuthScheme          string
	IrodsNegotiationPolicy   string
	IrodsSSLConfig           IrodsSSLConfig
	IrodsDefaultResource     string
	TestResource1            string
	TestResource2            string
	ResourceAffinity         []string
	S3BucketMappingFile      string
	ReplicaTrimMinCopies     int
	ReplicaTrimMinAgeMinutes int
	OidcUrl                  string
	OidcClientId             string
	OidcClientSecret         string
	OidcClientSecretFile     string
	OidcInsecureSkipVerify   bool
	OidcRealm                string
	OidcScope                string
}

type IrodsSSLConfig struct {
	CACertificateFile       string
	CACertificatePath       string
	EncryptionKeySize       int
	EncryptionAlgorithm     string
	EncryptionSaltSize      int
	EncryptionNumHashRounds int
	VerifyServer            string
	DHParamsFile            string
	ServerName              string
}

func NormalizeIRODSNegotiationPolicy(policy string) string {
	policy = strings.TrimSpace(policy)
	switch policy {
	case string(types.CSNegotiationPolicyRequestTCP), string(types.CSNegotiationPolicyRequestSSL), string(types.CSNegotiationPolicyRequestDontCare):
		return policy
	default:
		slog.Warn(
			"invalid iRODS negotiation policy; defaulting to CS_NEG_DONT_CARE",
			"configured_policy", policy,
			"default_policy", string(types.CSNegotiationPolicyRequestDontCare),
		)
		return string(types.CSNegotiationPolicyRequestDontCare)
	}
}

func (cfg *RestConfig) AdminAuthScheme() types.AuthScheme {
	adminLoginType := strings.TrimSpace(cfg.IrodsAdminLoginType)
	if adminLoginType == "" {
		adminLoginType = strings.TrimSpace(cfg.IrodsAuthScheme)
	}

	return types.GetAuthScheme(adminLoginType)
}

func (cfg *RestConfig) RequestAuthScheme() types.AuthScheme {
	return types.GetAuthScheme(cfg.IrodsAuthScheme)
}

func (cfg *RestConfig) ToIRODSSSLConfig() *types.IRODSSSLConfig {
	sslConfig := cfg.IrodsSSLConfig

	verifyServerName := strings.TrimSpace(sslConfig.VerifyServer)
	if verifyServerName == "" {
		verifyServerName = defaultIRODSSSLVerifyServer
	}

	verifyServer, err := types.GetSSLVerifyServer(verifyServerName)
	if err != nil {
		slog.Warn(
			"invalid iRODS SSL verify server; defaulting to go-irodsclient value",
			"configured_verify_server", verifyServerName,
			"default_verify_server", defaultIRODSSSLVerifyServer,
		)
		verifyServer, _ = types.GetSSLVerifyServer(defaultIRODSSSLVerifyServer)
	}

	encryptionKeySize := sslConfig.EncryptionKeySize
	if encryptionKeySize <= 0 {
		encryptionKeySize = defaultIRODSEncryptionKeySize
	}

	encryptionAlgorithm := strings.TrimSpace(sslConfig.EncryptionAlgorithm)
	if encryptionAlgorithm == "" {
		encryptionAlgorithm = defaultIRODSEncryptionAlgorithm
	}

	encryptionSaltSize := sslConfig.EncryptionSaltSize
	if encryptionSaltSize <= 0 {
		encryptionSaltSize = defaultIRODSEncryptionSaltSize
	}

	encryptionNumHashRounds := sslConfig.EncryptionNumHashRounds
	if encryptionNumHashRounds <= 0 {
		encryptionNumHashRounds = defaultIRODSEncryptionNumHashRounds
	}

	return &types.IRODSSSLConfig{
		CACertificateFile:       strings.TrimSpace(sslConfig.CACertificateFile),
		CACertificatePath:       strings.TrimSpace(sslConfig.CACertificatePath),
		EncryptionKeySize:       encryptionKeySize,
		EncryptionAlgorithm:     encryptionAlgorithm,
		EncryptionSaltSize:      encryptionSaltSize,
		EncryptionNumHashRounds: encryptionNumHashRounds,
		VerifyServer:            verifyServer,
		DHParamsFile:            strings.TrimSpace(sslConfig.DHParamsFile),
		ServerName:              strings.TrimSpace(sslConfig.ServerName),
	}
}

func (cfg *RestConfig) ApplyIRODSConnectionConfig(account *types.IRODSAccount) *types.IRODSAccount {
	if account == nil {
		return nil
	}

	normalizedPolicy := NormalizeIRODSNegotiationPolicy(cfg.IrodsNegotiationPolicy)
	negotiationPolicy := types.GetCSNegotiationPolicyRequest(normalizedPolicy)
	requireNegotiation := negotiationPolicy == types.CSNegotiationPolicyRequestSSL

	account.SetCSNegotiation(requireNegotiation, negotiationPolicy)
	account.SetSSLConfiguration(cfg.ToIRODSSSLConfig())
	account.FixAuthConfiguration()

	return account
}

func (cfg *RestConfig) ToIrodsAccount() types.IRODSAccount {
	account := types.IRODSAccount{
		AuthenticationScheme: cfg.AdminAuthScheme(),
		Host:                 cfg.IrodsHost,
		Port:                 cfg.IrodsPort,
		ClientUser:           cfg.IrodsAdminUser,
		ClientZone:           cfg.IrodsZone,
		ProxyUser:            cfg.IrodsAdminUser,
		ProxyZone:            cfg.IrodsZone,
		Password:             cfg.IrodsAdminPassword,
		DefaultResource:      cfg.IrodsDefaultResource,
	}

	cfg.ApplyIRODSConnectionConfig(&account)

	return account
}

const DefaultConfigName = "rest-config"
const DefaultConfigType = "yaml"
const ConfigFileEnvVar = "IRODS_REST_CONFIG_FILE"
const defaultIRODSEncryptionAlgorithm = "AES-256-CBC"
const defaultIRODSEncryptionKeySize = 32
const defaultIRODSEncryptionSaltSize = 8
const defaultIRODSEncryptionNumHashRounds = 16
const defaultIRODSSSLVerifyServer = "hostname"

func bindEnvVars(v *viper.Viper) error {
	envBindings := map[string][]string{
		"PublicURL":                              {"GOREST_PUBLIC_URL", "GOREST_PUBLICURL"},
		"ListenAddr":                             {"IRODS_REST_ADDR", "GOREST_LISTEN_ADDR", "GOREST_LISTENADDR"},
		"RestLogLevel":                           {"GOREST_REST_LOG_LEVEL", "GOREST_RESTLOGLEVEL"},
		"IrodsHost":                              {"GOREST_IRODS_HOST", "GOREST_IRODSHOST"},
		"IrodsPort":                              {"GOREST_IRODS_PORT", "GOREST_IRODSPORT"},
		"IrodsZone":                              {"GOREST_IRODS_ZONE", "GOREST_IRODSZONE"},
		"IrodsAdminUser":                         {"GOREST_IRODS_ADMIN_USER", "GOREST_IRODSADMINUSER"},
		"IrodsAdminPassword":                     {"GOREST_IRODS_ADMIN_PASSWORD", "GOREST_IRODSADMINPASSWORD"},
		"IrodsAdminPasswordFile":                 {"GOREST_IRODS_ADMIN_PASSWORD_FILE", "GOREST_IRODSADMINPASSWORDFILE"},
		"IrodsAdminLoginType":                    {"GOREST_IRODS_ADMIN_LOGIN_TYPE", "GOREST_IRODS_ADMIN_AUTH_SCHEME", "GOREST_IRODSADMINLOGINTYPE"},
		"IrodsAuthScheme":                        {"GOREST_IRODS_AUTH_SCHEME", "GOREST_IRODSAUTHSCHEME"},
		"IrodsNegotiationPolicy":                 {"GOREST_IRODS_NEGOTIATION_POLICY", "GOREST_IRODSNEGOTIATIONPOLICY"},
		"IrodsSSLConfig.CACertificateFile":       {"GOREST_IRODS_SSL_CA_CERTIFICATE_FILE", "GOREST_IRODSSSLCACERTIFICATEFILE"},
		"IrodsSSLConfig.CACertificatePath":       {"GOREST_IRODS_SSL_CA_CERTIFICATE_PATH", "GOREST_IRODSSSLCACERTIFICATEPATH"},
		"IrodsSSLConfig.EncryptionKeySize":       {"GOREST_IRODS_ENCRYPTION_KEY_SIZE", "GOREST_IRODSENCRYPTIONKEYSIZE"},
		"IrodsSSLConfig.EncryptionAlgorithm":     {"GOREST_IRODS_ENCRYPTION_ALGORITHM", "GOREST_IRODSENCRYPTIONALGORITHM"},
		"IrodsSSLConfig.EncryptionSaltSize":      {"GOREST_IRODS_ENCRYPTION_SALT_SIZE", "GOREST_IRODSENCRYPTIONSALTSIZE"},
		"IrodsSSLConfig.EncryptionNumHashRounds": {"GOREST_IRODS_ENCRYPTION_NUM_HASH_ROUNDS", "GOREST_IRODSENCRYPTIONNUMHASHROUNDS"},
		"IrodsSSLConfig.VerifyServer":            {"GOREST_IRODS_SSL_VERIFY_SERVER", "GOREST_IRODSSSLVERIFYSERVER"},
		"IrodsSSLConfig.DHParamsFile":            {"GOREST_IRODS_SSL_DH_PARAMS_FILE", "GOREST_IRODSSSLDHPARAMSFILE"},
		"IrodsSSLConfig.ServerName":              {"GOREST_IRODS_SSL_SERVER_NAME", "GOREST_IRODSSSLSERVERNAME"},
		"IrodsDefaultResource":                   {"GOREST_IRODS_DEFAULT_RESOURCE", "GOREST_IRODSDEFAULTRESOURCE"},
		"TestResource1":                          {"GOREST_TEST_RESOURCE1"},
		"TestResource2":                          {"GOREST_TEST_RESOURCE2"},
		"ResourceAffinity":                       {"GOREST_RESOURCE_AFFINITY", "GOREST_RESOURCEAFFINITY"},
		"S3BucketMappingFile":                    {"GOREST_S3_BUCKET_MAPPING_FILE", "GOREST_S3BUCKETMAPPINGFILE"},
		"ReplicaTrimMinCopies":                   {"GOREST_REPLICA_TRIM_MIN_COPIES"},
		"ReplicaTrimMinAgeMinutes":               {"GOREST_REPLICA_TRIM_MIN_AGE_MINUTES"},
		"OidcUrl":                                {"GOREST_OIDC_URL", "GOREST_OIDCURL"},
		"OidcClientId":                           {"GOREST_OIDC_CLIENT_ID", "GOREST_OIDCCLIENTID"},
		"OidcClientSecret":                       {"GOREST_OIDC_CLIENT_SECRET", "GOREST_OIDCCLIENTSECRET"},
		"OidcClientSecretFile":                   {"GOREST_OIDC_CLIENT_SECRET_FILE", "GOREST_OIDCCLIENTSECRETFILE"},
		"OidcInsecureSkipVerify":                 {"GOREST_OIDC_INSECURE_SKIP_VERIFY", "GOREST_OIDCINSECURESKIPVERIFY"},
		"OidcRealm":                              {"GOREST_OIDC_REALM", "GOREST_OIDCREALM"},
		"OidcScope":                              {"GOREST_OIDC_SCOPE", "GOREST_OIDCSCOPE"},
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

	configName = strings.TrimSpace(configName)
	configType = strings.TrimSpace(configType)

	if configFilePath := strings.TrimSpace(os.Getenv(ConfigFileEnvVar)); configFilePath != "" {
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
			path = strings.TrimSpace(path)
			if path == "" {
				continue
			}

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

	err = v.Unmarshal(&C, func(decoderConfig *mapstructure.DecoderConfig) {
		decoderConfig.DecodeHook = mapstructure.ComposeDecodeHookFunc(
			mapstructure.StringToSliceHookFunc(","),
			func(from reflect.Type, to reflect.Type, data any) (any, error) {
				if to.Kind() != reflect.Slice || to.Elem().Kind() != reflect.String {
					return data, nil
				}

				values, ok := data.([]string)
				if !ok {
					return data, nil
				}

				normalized := make([]string, 0, len(values))
				for _, value := range values {
					value = strings.TrimSpace(value)
					if value == "" {
						continue
					}
					normalized = append(normalized, value)
				}
				return normalized, nil
			},
		)
	})
	if err != nil {
		return nil, fmt.Errorf("unable to decode into struct: %w", err)
	}

	C.IrodsAdminPassword, err = resolveSecret(C.IrodsAdminPassword, C.IrodsAdminPasswordFile, "iRODS admin password")
	if err != nil {
		return nil, err
	}

	C.IrodsNegotiationPolicy = NormalizeIRODSNegotiationPolicy(C.IrodsNegotiationPolicy)
	if C.ReplicaTrimMinCopies <= 0 {
		C.ReplicaTrimMinCopies = 1
	}
	if C.ReplicaTrimMinAgeMinutes < 0 {
		slog.Warn(
			"invalid replica trim minimum age; defaulting to 0",
			"configured_min_age_minutes", C.ReplicaTrimMinAgeMinutes,
			"default_min_age_minutes", 0,
		)
		C.ReplicaTrimMinAgeMinutes = 0
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
