package validator

import (
	"fmt"
	"reflect"
)

// FieldNotEmpty checks if a specific field in the data is not empty
type FieldNotEmpty struct {
	Field string
}

// Validate checks if the field in the data is empty
func (v *FieldNotEmpty) Validate(data any) error {
	// Convert data to a slice of maps
	var dataSlice []map[string]any
	switch v := data.(type) {
	case []map[string]any:
		dataSlice = v
	case map[string]any:
		dataSlice = []map[string]any{v}
	case []any:
		for _, d := range v {
			switch d := d.(type) {
			case map[string]any:
				dataSlice = append(dataSlice, d)
			}
		}
	default:
		fmt.Println(reflect.TypeOf(data))
		return fmt.Errorf("unsupported data type for validation")
	}

	// Check each record in the data
	for _, record := range dataSlice {
		if value, ok := record[v.Field]; !ok || value == "" {
			return fmt.Errorf("field '%s' is empty in record", v.Field)
		}
	}
	return nil
}
