package v2

import (
	"fmt"
	"time"
)

type Source interface {
	Read() (any, error)
}

type Target interface {
	Write(data any) error
}

type DataSource interface {
	Source
	Target
}

type Transformer interface {
	Transform(data any) (any, error)
}

type Validator interface {
	Validate(data any) error
}

type ETL struct {
	Source       Source
	Transformers []Transformer
	Target       Target
	Validators   []Validator
	Logger       Logger
	ErrorHandler ErrorHandler
	Config       Config
	Metrics      Metrics
}

func (e *ETL) Log(step, message string) {
	if e.Logger != nil {
		e.Logger.Log(step, message)
	}
}

func (e *ETL) HandleError(step string, err error) bool {
	if e.ErrorHandler != nil {
		return e.ErrorHandler.Handle(step, err)
	}
	return false
}

type Logger interface {
	Log(step string, message string)
}

type ErrorHandler interface {
	Handle(step string, err error) bool
}

type Config struct {
	BatchSize      int
	MaxRetries     int
	RetryBackoffMs int
}

type Metrics struct {
	RecordsProcessed int
	ErrorsOccurred   int
	Duration         time.Duration
}

func (e *ETL) Execute() error {
	e.Log("ETL", "Starting ETL process")
	startTime := time.Now()
	data, err := e.Source.Read()
	if err != nil {
		e.Log("Source", fmt.Sprintf("Error: %v", err))
		if !e.HandleError("Source", err) {
			return err
		}
	}
	var processedData any
	switch v := data.(type) {
	case []any:
		processedData = v
	case map[string]interface{}:
		processedData = []any{v}
	case []map[string]any:
		processedData = v
	default:
		processedData = []any{v}
	}
	if err := e.Validate(processedData); err != nil {
		e.Log("Validation", fmt.Sprintf("Error: %v", err))
		if !e.HandleError("Validation", err) {
			return err
		}
	}
	for _, transformer := range e.Transformers {
		bData, err := transformer.Transform(processedData)
		if err != nil {
			e.Log("Transformer", fmt.Sprintf("Error: %v", err))
			if !e.HandleError("Transformer", err) {
				return err
			}
		}
		if err := e.Target.Write(bData); err != nil {
			e.Log("Target", fmt.Sprintf("Error: %v", err))
			if !e.HandleError("Target", err) {
				return err
			}
			return err
		}
	}
	e.Metrics.Duration = time.Since(startTime)
	e.Log("ETL", fmt.Sprintf("ETL process completed successfully in %v", e.Metrics.Duration))
	return nil
}

func (e *ETL) Validate(data any) error {
	for _, validator := range e.Validators {
		if err := validator.Validate(data); err != nil {
			return err
		}
	}
	return nil
}
