package v2

import (
	"fmt"
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

type ETL struct {
	Source       Source
	Transformers []Transformer
	Target       Target
	Logger       Logger
	ErrorHandler ErrorHandler
	Config       Config
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

func (e *ETL) Execute() error {
	e.Log("ETL", "Starting ETL process")
	data, err := e.Source.Read()
	if err != nil {
		e.Log("Source", fmt.Sprintf("Error: %v", err))
		if !e.HandleError("Source", err) {
			return err
		}
	}
	for _, transformer := range e.Transformers {
		data, err = transformer.Transform(data)
		if err != nil {
			e.Log("Transformer", fmt.Sprintf("Error: %v", err))
			if !e.HandleError("Transformer", err) {
				return err
			}
		}
	}
	err = e.Target.Write(data)
	if err != nil {
		e.Log("Target", fmt.Sprintf("Error: %v", err))
		if !e.HandleError("Target", err) {
			return err
		}
	}
	e.Log("ETL", "ETL process completed successfully")
	return nil
}
