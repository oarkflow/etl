package mapper

import (
	"fmt"
	"strings"
	"sync"

	"github.com/oarkflow/pkg/dipper"
	"github.com/oarkflow/pkg/evaluate"

	"github.com/oarkflow/etl"
)

type Config struct {
	FieldMaps           map[string]string
	Lookups             any
	KeepUnmatchedFields bool
}

type Mapper struct {
	cfg         *Config
	lookupCache map[string]map[string]any
	mu          *sync.RWMutex
}

func (m *Mapper) Name() string {
	return "mapper"
}

func (m *Mapper) Transform(data etl.Data) error {
	switch data := data.(type) {
	case map[string]any:
		var fields []string
		for f := range data {
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
		if !m.cfg.KeepUnmatchedFields && len(m.cfg.FieldMaps) > 0 {
			for k := range data {
				if _, ok := m.cfg.FieldMaps[k]; !ok {
					delete(data, k)
				}
			}
		}
	}
	return nil
}

func (m *Mapper) lookupIn(ctx evaluate.EvalContext) (interface{}, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.CheckArgCount(4); err != nil {
		return nil, err
	}
	arg1, err := ctx.Arg(0)
	if err != nil {
		return nil, err
	}
	arg2, err := ctx.Arg(1)
	if err != nil {
		return nil, err
	}
	arg3, err := ctx.Arg(2)
	if err != nil {
		return nil, err
	}
	arg4, err := ctx.Arg(3)
	if err != nil {
		return nil, err
	}
	lookup := arg1.(string)
	key := arg2.(string)
	value := arg3
	fieldToRetrieve := arg4.(string)
	k := fmt.Sprintf("%s.%v.%s", key, value, fieldToRetrieve)
	if cache, exists := m.lookupCache[lookup]; exists {
		if data, exists := cache[k]; exists {
			return data, nil
		}
	}
	if m.cfg.Lookups != nil {
		switch lookupData := m.cfg.Lookups.(type) {
		case map[string]any:
			if rows, ok := lookupData[lookup]; ok {
				data := dipper.FilterSlice(rows, ".[]."+key, []any{value})
				switch data := data.(type) {
				case error:
					return nil, data
				case []any:
					if len(data) > 0 {
						d := data[0]
						switch d := d.(type) {
						case map[string]any:
							m.lookupCache[lookup][k] = d[fieldToRetrieve]
							return d[fieldToRetrieve], nil
						}
					}
				case []map[string]any:
					if len(data) > 0 {
						d := data[0]
						m.lookupCache[lookup][k] = d[fieldToRetrieve]
						return d[fieldToRetrieve], nil
					}
				}
			}
		}
	}
	return nil, nil
}

func New(cfg *Config) *Mapper {
	m := &Mapper{
		cfg:         cfg,
		lookupCache: make(map[string]map[string]any),
		mu:          &sync.RWMutex{},
	}
	if m.cfg.Lookups != nil {
		switch lookupData := m.cfg.Lookups.(type) {
		case map[string]any:
			for entity, _ := range lookupData {
				m.lookupCache[entity] = make(map[string]any)
			}
		}
	}
	evaluate.AddCustomOperator("lookupIn", m.lookupIn)
	return m
}
