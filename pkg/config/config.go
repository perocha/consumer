package config

import (
	"os"

	"github.com/perocha/goutils/pkg/config"
)

type MicroserviceConfig struct {
	configClient                     *config.Config
	AppInsightsInstrumentationKey    string
	EventHubNameConsumer             string
	EventHubConsumerConnectionString string
	CheckpointStoreContainerName     string
	CheckpointStoreConnectionString  string
}

// Initialize configuration client, either from environment variable or from file
func InitializeConfig() (*MicroserviceConfig, error) {
	// First try to get the App Configuration connection string as environment variable
	connectionString := os.Getenv("APPCONFIGURATION_CONNECTION_STRING")

	if connectionString != "" {
		mycfg, err := config.NewConfigFromConnectionString(connectionString)
		if err != nil {
			return nil, err
		}
		return &MicroserviceConfig{
			configClient: mycfg,
		}, nil
	} else {
		fileName := "config.yaml"
		mycfg, err := config.NewConfigFromFile(fileName)
		if err != nil {
			return nil, err
		}
		return &MicroserviceConfig{
			configClient: mycfg,
		}, nil
	}
}

// Refresh configuration, with the latest values from the configuration store
func (cfg *MicroserviceConfig) RefreshConfig() error {
	if err := retrieveConfigValue(cfg, "APPINSIGHTS_INSTRUMENTATIONKEY", &cfg.AppInsightsInstrumentationKey); err != nil {
		return err
	}

	if err := retrieveConfigValue(cfg, "EVENTHUB_CONSUMER_CONNECTION_STRING", &cfg.EventHubConsumerConnectionString); err != nil {
		return err
	}

	if err := retrieveConfigValue(cfg, "CHECKPOINTSTORE_CONTAINER_NAME", &cfg.CheckpointStoreContainerName); err != nil {
		return err
	}

	if err := retrieveConfigValue(cfg, "CHECKPOINTSTORE_STORAGE_CONNECTION_STRING", &cfg.CheckpointStoreConnectionString); err != nil {
		return err
	}

	return nil
}

// RetrieveConfigValue retrieves a configuration value from the client and sets it in the target field.
func retrieveConfigValue(cfg *MicroserviceConfig, key string, target *string) error {
	configValue, err := cfg.configClient.GetVar(key)
	if err != nil {
		return err
	}
	*target = configValue
	return nil
}
