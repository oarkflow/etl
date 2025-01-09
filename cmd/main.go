package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/oarkflow/metadata"
	"gopkg.in/yaml.v3"

	"github.com/oarkflow/etl/migrate"
)

var (
	file = flag.String("file", "loader.json", "File to migrate database")
)

func main() {
	flag.Parse()
	if *file == "" {
		log.Fatal("Config file cannot be empty")
	}
	var cfg *Config
	var err error
	switch filepath.Ext(*file) {
	case ".yaml":
		cfg, err = loadYaml(*file)
	case ".json":
		cfg, err = loadJson(*file)
	default:
		log.Fatalf("Unsupported file format, requires yaml or json file")
	}
	if err != nil {
		log.Fatal(err)
	}
	if len(cfg.Files) > 0 {
		for _, f := range cfg.Files {
			err := ProcessFile(filepath.Join(cfg.Path, f))
			if err != nil {
				log.Fatal(err)
			}
		}
	} else {
		err = migrate.Data(cfg.Source, cfg.Destination, cfg.Tables)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func ProcessFile(file string) (err error) {
	var cfg *Config
	switch filepath.Ext(file) {
	case ".yaml":
		cfg, err = loadYaml(file)
	case ".json":
		cfg, err = loadJson(file)
	default:
		log.Fatalf("Unsupported file format, requires yaml or json file")
	}
	if err != nil {
		return err
	}
	return migrate.Data(cfg.Source, cfg.Destination, cfg.Tables)
}

type Config struct {
	Source      metadata.Config       `json:"source" yaml:"source"`
	Destination metadata.Config       `json:"destination" yaml:"destination"`
	Tables      []migrate.TableConfig `json:"tables" yaml:"tables"`
	Files       []string              `json:"files" yaml:"files"`
	Path        string                `json:"path" yaml:"path"`
}

// loadJson loads the yaml configuration file and returns a Config struct.
func loadJson(filename string) (*Config, error) {
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

// loadYaml loads the yaml configuration file and returns a Config struct.
func loadYaml(filename string) (*Config, error) {
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
