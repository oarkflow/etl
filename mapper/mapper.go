package mapper

import (
	"github.com/oarkflow/etl"
)

type Config struct {
	FieldMaps           map[string]string
	KeepUnmatchedFields bool
}

type Mapper struct {
	cfg *Config
}

func (m *Mapper) Name() string {
	return "mapper"
}

func (m *Mapper) Transform(data etl.Data) error {
	switch data := data.(type) {
	case map[string]any:
		for field, value := range m.cfg.FieldMaps {
			if val, ok := data[field]; ok {
				data[value] = val
				delete(data, field)
			} else if !m.cfg.KeepUnmatchedFields {
				delete(data, field)
			}
		}
	}
	return nil
}

func New(cfg *Config) *Mapper {
	return &Mapper{
		cfg: cfg,
	}
}
