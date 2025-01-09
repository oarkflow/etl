package transformer

import (
	"fmt"
)

type FieldMapper struct {
	Mapping map[string]string
}

func (f *FieldMapper) Transform(data any) (any, error) {
	if rows, ok := data.([]map[string]any); ok {
		for _, row := range rows {
			for oldKey, newKey := range f.Mapping {
				if val, exists := row[oldKey]; exists {
					row[newKey] = val
					delete(row, oldKey)
				}
			}
		}
		return rows, nil
	}
	return nil, fmt.Errorf("unexpected data type: %T", data)
}
