package transformer

import (
	"fmt"
	"reflect"
	"strings"
)

type UpperCase struct{}

func (u *UpperCase) Transform(data any) (any, error) {
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
	return nil, fmt.Errorf("unexpected data type: %v", reflect.TypeOf(data))
}
