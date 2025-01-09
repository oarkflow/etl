package transformer

import (
	"fmt"
	"reflect"
	"strings"
)

type UpperCase struct{}

func (u *UpperCase) Transform(data any) (any, error) {
	switch rows := data.(type) {
	case []map[string]any:
		for _, row := range rows {
			for key, value := range row {
				if strVal, ok := value.(string); ok {
					row[key] = strings.ToUpper(strVal)
				}
			}
		}
		return rows, nil
	case []any:
		for _, row := range rows {
			switch row := row.(type) {
			case map[string]any:
				for key, value := range row {
					if strVal, ok := value.(string); ok {
						row[key] = strings.ToUpper(strVal)
					}
				}
			}
		}
		return rows, nil
	}
	if rows, ok := data.([]map[string]any); ok {
		for _, row := range rows {
			for key, value := range row {
				if strVal, ok := value.(string); ok {
					row[key] = strings.ToUpper(strVal)
				}
			}
		}
		return rows, nil
	}
	return nil, fmt.Errorf("Upper mapper: unexpected data type: %v", reflect.TypeOf(data))
}
