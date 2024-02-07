package main

import (
	"fmt"
	"os"

	"github.com/oarkflow/etl/migrate"

	"github.com/oarkflow/metadata"
	"gopkg.in/yaml.v3"
)

// Config is the top-level configuration struct.
// It contains the source and destination database configurations, as well as a list of tables to
// migrate.
type Config struct {
	Source      metadata.Config       `json:"source"`
	Destination metadata.Config       `json:"destination"`
	Tables      []migrate.TableConfig `json:"tables"`
}

// loadConfig loads the yaml configuration file and returns a Config struct.
func loadConfig(filename string) (*Config, error) {
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

// main loads the configuration file and migrates all tables.
func main() {
	config, err := loadConfig("users_config.yaml")
	if err != nil {
		panic(err)
	}
	err = migrate.Data(config.Source, config.Destination, config.Tables)
	if err != nil {
		panic(err)
	}
}
