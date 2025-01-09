package v2

import (
	"encoding/json"
	"os"
)

// JSON reads data from a JSON file
type JSON struct {
	FilePath string
}

func (j *JSON) Read() (any, error) {
	file, err := os.Open(j.FilePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var data any
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (j *JSON) Write(data any) error {
	file, err := os.Create(j.FilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "\t")
	return encoder.Encode(data)
}
