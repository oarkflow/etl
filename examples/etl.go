package main

import (
	"fmt"

	"github.com/oarkflow/etl/v2/validator"

	"github.com/oarkflow/etl/v2"
	"github.com/oarkflow/etl/v2/transformer"
)

func main() {
	// Define the source and target
	source := &v2.CSV{FilePath: "input.csv"}
	target := &v2.JSON{FilePath: "output.json"}

	// Define the transformations (uppercasing and field mapping)
	mappers := []v2.Transformer{
		&transformer.UpperCase{},
		&transformer.FieldMapper{Mapping: map[string]string{
			"old_field": "new_field",
		}},
	}

	// Define validations (can add multiple validation steps if needed)
	validators := []v2.Validator{
		// Example of a validator (you can create custom validators as needed)
		&validator.FieldNotEmpty{Field: "name"},
	}

	// Create the ETL object
	etl := &v2.ETL{
		Source:       source,
		Transformers: mappers,
		Target:       target,
		Validators:   validators,
		Config: v2.Config{
			BatchSize: 2, // For example, batch size of 2
		},
	}

	// Execute the ETL process
	if err := etl.Execute(); err != nil {
		fmt.Printf("ETL process failed: %v\n", err)
	} else {
		fmt.Println("ETL process completed successfully!")
	}
}
