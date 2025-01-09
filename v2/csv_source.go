package v2

import (
	"encoding/csv"
	"io"
	"os"
)

// CSV reads data from a CSV file
type CSV struct {
	FilePath string
}

func (c *CSV) Write(data any) error {
	// TODO implement me
	panic("implement me")
}

func (c *CSV) Read() (any, error) {
	file, err := os.Open(c.FilePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	headers, err := reader.Read()
	if err != nil {
		return nil, err
	}

	var data []map[string]any
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		row := make(map[string]any)
		for i, header := range headers {
			row[header] = record[i]
		}
		data = append(data, row)
	}
	return data, nil
}
