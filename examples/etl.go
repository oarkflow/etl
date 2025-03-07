package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/oarkflow/metadata/utils"

	v2 "github.com/oarkflow/etl/v2"
	"github.com/oarkflow/etl/v2/contracts"
	"github.com/oarkflow/etl/v2/loader"
	"github.com/oarkflow/etl/v2/mapper"
	"github.com/oarkflow/etl/v2/source"
)

type LowercaseMapper struct{}

func (m *LowercaseMapper) Name() string {
	return "Lower"
}

func (m *LowercaseMapper) Map(_ context.Context, rec utils.Record) (utils.Record, error) {
	newRec := make(utils.Record)
	for k, v := range rec {
		newRec[strings.ToLower(k)] = v
	}
	return newRec, nil
}

type VotingTransformer struct{}

func (t *VotingTransformer) Transform(_ context.Context, rec utils.Record) (utils.Record, error) {
	if ageStr, ok := rec["age"].(string); ok {
		age, err := strconv.Atoi(ageStr)
		if err != nil {
			return rec, fmt.Errorf("invalid age value: %v", err)
		}
		rec["allowed_voting"] = age > 18
	}
	return rec, nil
}

type RequiredFieldValidator struct {
	Field string
}

func (v *RequiredFieldValidator) Validate(_ context.Context, rec utils.Record) error {
	if val, ok := rec[v.Field]; !ok || fmt.Sprintf("%v", val) == "" {
		return fmt.Errorf("required field '%s' is missing or empty", v.Field)
	}
	return nil
}

type FileCheckpointStore struct {
	fileName string
	mu       sync.Mutex
}

func (cs *FileCheckpointStore) SaveCheckpoint(_ context.Context, cp string) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return os.WriteFile(cs.fileName, []byte(cp), 0644)
}

func (cs *FileCheckpointStore) GetCheckpoint(context.Context) (string, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	data, err := os.ReadFile(cs.fileName)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	return string(data), nil
}

func main() {
	csvSource := source.NewFileSource("input.csv")
	jsonLoader := loader.NewFileLoader("output.json", true)
	fieldMapper := mapper.NewFieldMapper(
		map[string]string{
			"age":  "age",
			"name": "name",
		},
		map[string]any{
			"voting_status": "pending",
		},
		false,
	)
	lowercaseMapper := &LowercaseMapper{}
	ageTransformer := &VotingTransformer{}
	requiredValidator := &RequiredFieldValidator{Field: "name"}
	checkpointStore := &FileCheckpointStore{fileName: "checkpoint.txt"}
	etlInstance := v2.NewETL(
		v2.WithSources(csvSource),
		v2.WithLoaders([]contracts.Loader{jsonLoader}...),
		v2.WithMappers(lowercaseMapper, fieldMapper),
		v2.WithTransformers([]contracts.Transformer{ageTransformer}...),
		v2.WithValidators([]contracts.Validator{requiredValidator}...),
		v2.WithCheckpointStore(checkpointStore, func(rec utils.Record) string {
			if name, ok := rec["name"].(string); ok {
				return name
			}
			return ""
		}),
		v2.WithRawChanBuffer(50),
		v2.WithWorkerCount(2),
		v2.WithBatchSize(5),
	)
	if err := etlInstance.Run(context.Background()); err != nil {
		log.Fatalf("ETL run failed: %v", err)
	}
	if err := etlInstance.Close(); err != nil {
		log.Fatalf("Error closing ETL: %v", err)
	}
	log.Println("ETL processing complete.")
}
