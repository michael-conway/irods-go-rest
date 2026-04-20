package config

import (
	"fmt"

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
	IrodsAuthScheme        string
	IrodsNegotiationPolicy string
	IrodsDefaultResource   string
	OidcUrl                string
	OidcClientId           string
	OidcClientSecret       string
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

// ReadRestConfig reads the configuration for REST behaviors in irods
// can take a number of paths that will be prefixed in the searh path, or defaults
// may be accepted, blank params for name and type default to irods-drs.yaml
func ReadRestConfig(configName string, configType string, configPaths []string) (*RestConfig, error) {

	if configName == "" {
		viper.SetConfigName(DefaultConfigName) // name of config file (without extension)
	} else {
		viper.SetConfigName(configName)
	}

	if configType == "" {
		viper.SetConfigType(DefaultConfigType) // REQUIRED if the config file does not have the extension in the name
	} else {
		viper.SetConfigType(configType)
	}

	for _, path := range configPaths {
		viper.AddConfigPath(path)
	}

	viper.AddConfigPath("/etc/irods-ext/")      // path to look for the config file in
	viper.AddConfigPath("$HOME/.irods-go-rest") // call multiple times to add many search paths
	viper.AddConfigPath(".")                    // optionally look for config in the working directory

	// Enable environment variable support with prefix GOREST_
	viper.SetEnvPrefix("GOREST")
	viper.AutomaticEnv()

	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		panic(fmt.Errorf("fatal error config file: %w", err))
	}
	var C RestConfig

	err = viper.Unmarshal(&C)
	if err != nil {
		panic(fmt.Errorf("unable to decode into struct, %v", err))
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
