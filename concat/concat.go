package concat

import (
	"fmt"
	"strings"

	"github.com/oarkflow/etl"
)

type Config struct {
	Delimiter        string
	SourceFields     []string
	DestinationField string
	KeepSourceFields bool
}

type Concat struct {
	cfg *Config
}

func (m *Concat) Name() string {
	return "concat"
}

func (m *Concat) Transform(data etl.Data) error {
	switch data := data.(type) {
	case map[string]any:
		var values []string
		for _, field := range m.cfg.SourceFields {
			if val, ok := data[field]; ok {
				values = append(values, fmt.Sprintf("%v", val))
			}
		}
		if m.cfg.DestinationField != "" {
			data[m.cfg.DestinationField] = strings.Join(values, m.cfg.Delimiter)
		}
		if !m.cfg.KeepSourceFields {
			for _, field := range m.cfg.SourceFields {
				if _, ok := data[field]; ok {
					delete(data, field)
				}
			}
		}
	}
	return nil
}

func New(cfg *Config) *Concat {
	if cfg.Delimiter == "" {
		cfg.Delimiter = " "
	}
	return &Concat{
		cfg: cfg,
	}
}
