package main

import (
	"fmt"

	v2 "github.com/oarkflow/etl/v2"
	"github.com/oarkflow/etl/v2/transformer"
)

func main() {
	source := &v2.CSV{FilePath: "input.csv"}
	target := &v2.JSON{FilePath: "output.json"}
	mappers := []v2.Transformer{
		&transformer.UpperCase{},
		&transformer.FieldMapper{Mapping: map[string]string{
			"old_field": "new_field",
		}},
	}
	etl := &v2.ETL{
		Source:       source,
		Transformers: mappers,
		Target:       target,
	}
	if err := etl.Execute(); err != nil {
		fmt.Printf("ETL process failed: %v\n", err)
	} else {
		fmt.Println("ETL process completed successfully!")
	}
}
