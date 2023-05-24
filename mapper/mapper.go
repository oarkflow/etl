package mapper

import (
	"strings"

	"github.com/oarkflow/pkg/evaluate"

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
		var fields []string
		for f, _ := range data {
			fields = append(fields, f)
		}
		for dest, src := range m.cfg.FieldMaps {
			if strings.HasPrefix(src, "{{") {
				p, _ := evaluate.Parse(src, true)
				pr := evaluate.NewEvalParams(data)
				d, err := p.Eval(pr)
				if err == nil {
					data[dest] = d
				}
			} else if val, ok := data[src]; ok {
				data[dest] = val
			}
		}
		if !m.cfg.KeepUnmatchedFields {
			for k, _ := range data {
				found := false
				for field, _ := range m.cfg.FieldMaps {
					if k == field {
						found = true
						break
					}
				}
				if !found {
					delete(data, k)
				}
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
