package utils

import (
	"fmt"
	"os"

	metadata "github.com/oarkflow/metadata/v2"
	"gopkg.in/yaml.v3"

	"github.com/oarkflow/etl/migrate"
)

// Config is the top-level configuration struct.
// It contains the source and destination database configurations, as well as a list of tables to
// migrate.
type Config struct {
	Source      metadata.Config       `json:"source"`
	Destination metadata.Config       `json:"destination"`
	Tables      []migrate.TableConfig `json:"tables"`
}

// LoadConfig loads the yaml configuration file and returns a Config struct.
func LoadConfig(filename string) (*Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read configuration file: %w", err)
	}

	config := &Config{}
	err = yaml.Unmarshal(data, config)
	if err != nil {
		return nil, fmt.Errorf("failed to parse configuration file: %w", err)
	}

	return config, nil
}

func ProcessFile(filename string) error {
	config, err := LoadConfig(filename)
	if err != nil {
		return err
	}
	return migrate.Data(config.Source, config.Destination, config.Tables)
}
