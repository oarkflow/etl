package sanitize

import (
	"strings"

	"github.com/oarkflow/etl"
)

type Config struct {
	LowerKeys bool
}

type Sanitize struct {
	cfg Config
}

func New(cfg ...Config) *Sanitize {
	var config Config
	if len(cfg) > 0 {
		config = cfg[0]
	}
	return &Sanitize{cfg: config}
}

func (s *Sanitize) Name() string {
	return "sanitize"
}

func (s *Sanitize) Transform(data etl.Data) error {
	switch row := data.(type) {
	case map[string]any:
		for field, val := range row {
			lowerField := strings.ToLower(field)
			row[lowerField] = val
			if lowerField != field {
				delete(row, field)
			}
		}
	}
	return nil
}
