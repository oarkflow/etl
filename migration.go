package etl

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"
)

type MigrationInfo struct {
	Table          string    `json:"table"`
	PrimaryKey     string    `json:"primary_key"`
	LastInsertedID any       `json:"last_inserted_id"`
	LastInsertedAt time.Time `json:"last_inserted_at"`
}

func (m *MigrationInfo) Update() error {
	jsonBytes, err := json.Marshal(m)
	if err != nil {
		return err
	}

	fileName := fmt.Sprintf("%s.json", m.Table)
	err = os.WriteFile(fileName, jsonBytes, 0644)
	if err != nil {
		return err
	}

	log.Println("JSON data written to", fileName, "till", m.LastInsertedID)
	return nil
}
