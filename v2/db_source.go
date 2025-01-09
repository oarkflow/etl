package v2

import (
	"database/sql"
	"fmt"
	"strings"
)

// Database reads data from a database
type Database struct {
	DSN   string
	Query string
	Table string
}

func (d *Database) Read() (any, error) {
	db, err := sql.Open("mysql", d.DSN)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.Query(d.Query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var data []map[string]any
	for rows.Next() {
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		row := make(map[string]any)
		for i, col := range columns {
			row[col] = values[i]
		}
		data = append(data, row)
	}
	return data, nil
}

func (d *Database) Write(data any) error {
	db, err := sql.Open("mysql", d.DSN)
	if err != nil {
		return err
	}
	defer db.Close()

	if rows, ok := data.([]map[string]any); ok {
		for _, row := range rows {
			columns := []string{}
			values := []any{}
			placeholders := []string{}
			for col, val := range row {
				columns = append(columns, col)
				values = append(values, val)
				placeholders = append(placeholders, "?")
			}

			query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", d.Table, strings.Join(columns, ","), strings.Join(placeholders, ","))
			_, err := db.Exec(query, values...)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
