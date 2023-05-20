package etl

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/oarkflow/errors"
	"github.com/oarkflow/metadata"
	"gorm.io/gorm"
)

type Data any

type Transformer interface {
	Name() string
	Transform(data Data) error
}

type Source struct {
	Name        string
	Query       string
	Type        string
	Identifiers []string
}

type Destination struct {
	Name string
	Type string
}

type Config struct {
	RowLimit    int64
	BatchSize   int
	SkipTables  []string
	CloneTables []string
	CloneSource bool
	Persist     bool
}

type ETL struct {
	srcCon       metadata.DataSource
	src          Source
	transformers []Transformer
	destCon      metadata.DataSource
	dest         Destination
	cloneSource  bool
	rowLimit     int64
	batchSize    int
}

func New(cfg ...Config) *ETL {
	var config Config
	if len(cfg) > 0 {
		config = cfg[0]
	}
	if config.BatchSize <= 0 {
		config.BatchSize = 100
	}
	e := &ETL{
		cloneSource: config.CloneSource,
		rowLimit:    config.RowLimit,
		batchSize:   config.BatchSize,
	}
	return e
}

func (e *ETL) AddSource(con metadata.DataSource, src Source) *ETL {
	e.srcCon = con
	e.src = src
	return e
}

func (e *ETL) AddDestination(con metadata.DataSource, dest Destination) *ETL {
	e.destCon = con
	e.dest = dest
	return e
}

func (e *ETL) CloneSource(clone bool) *ETL {
	e.cloneSource = clone
	return e
}

func (e *ETL) AddTransformer(transformer ...Transformer) *ETL {
	if len(transformer) > 0 {
		e.transformers = append(e.transformers, transformer...)
	}
	return e
}

func (e *ETL) Process(payload ...[]map[string]any) ([]map[string]any, error) {
	var failedData []map[string]any
	if e.cloneSource {
		err := metadata.CloneTable(e.srcCon, e.destCon, e.src.Name, e.dest.Name)
		if err != nil {
			return nil, err
		}
	}
	if e.dest.Name == "" {
		e.dest.Name = e.src.Name
	}
	data, err := e.getData(payload...)
	if err != nil {
		return nil, errors.NewE(err, fmt.Sprintf("Unable to get data for %s", e.src.Name), "ETLTransform")
	}
	destFields, err := e.destCon.GetFields(e.dest.Name)
	if err != nil {
		if err != nil {
			return nil, errors.NewE(err, fmt.Sprintf("Unable to get field list for %s", e.dest.Name), "ETLTransform")
		}
	}
	var rows []map[string]any
	for _, row := range data {
		// Moved to Sanitize with LowerKeys config
		// for field, val := range row {
		// 	lowerField := strings.ToLower(field)
		// 	row[lowerField] = val
		// 	if lowerField != field {
		// 		delete(row, field)
		// 	}
		// }

		err = e.transform(row)
		if err != nil {
			return nil, err
		}
		for _, field := range destFields {
			fixFieldType(row, field)
		}
		if len(rows) > 0 && len(rows)%e.batchSize == 0 {
			err = e.destCon.Store(e.dest.Name, rows)
			if err != nil {
				if !errors.Is(err, gorm.ErrDuplicatedKey) {
					failedData = append(failedData, rows...)
				} else {
					panic(err)
				}
			}
			rows = []map[string]any{}
		} else {
			rows = append(rows, row)
		}

	}
	return failedData, nil
}

func (e *ETL) getData(payload ...[]map[string]any) ([]map[string]any, error) {
	if len(payload) > 0 {
		return payload[0], nil
	}
	err := connect(e.srcCon, e.destCon)
	if err != nil {
		return nil, err
	}
	if e.src.Name == "" {
		return nil, errors.New("source not defined")
	}
	fields, _ := e.srcCon.GetFields(e.src.Name)
	filter := make(map[string]any)
	sql := fmt.Sprintf("SELECT * FROM %s", e.src.Name)
	var field *metadata.Field
	for _, f := range fields {
		if strings.ToLower(f.DataType) == "serial" || strings.ToUpper(f.Extra) == "AUTO_INCREMENT" {
			field = &f
			break
		}
	}
	if field != nil {
		name := strings.ToLower(field.Name)
		sql += fmt.Sprintf(" ORDER BY %s", name)
	}
	if e.rowLimit > 0 {
		sql += fmt.Sprintf(" LIMIT %d", e.rowLimit)
	}
	return e.srcCon.GetRawCollection(sql, filter)
}

func (e *ETL) transform(row map[string]any) error {
	for _, transformer := range e.transformers {
		err := transformer.Transform(row)
		if err != nil {
			return errors.NewE(err, fmt.Sprintf("Unable to transform using %s", transformer.Name()), "ETLTransform")
		}
	}
	return nil
}

func MigrateDB(srcCon metadata.DataSource, destCon metadata.DataSource, config Config) error {
	err := connect(srcCon, destCon)
	if err != nil {
		return err
	}
	var tables []string
	sources, err := srcCon.GetTables()
	for _, src := range sources {
		if len(config.CloneTables) > 0 {
			if contains(config.CloneTables, src.Name) {
				tables = append(tables, src.Name)
			}
		}
		if len(config.SkipTables) > 0 {
			if !contains(config.SkipTables, src.Name) {
				tables = append(tables, src.Name)
			}
		} else {
			tables = append(tables, src.Name)
		}
	}
	for _, src := range tables {
		etl := New(Config{
			RowLimit:    config.RowLimit,
			CloneSource: true,
			Persist:     config.Persist,
		})
		_, err := etl.
			AddSource(srcCon, Source{Name: src}).
			AddDestination(destCon, Destination{}).
			Process()
		if err != nil {
			return err
		}
	}
	return nil
}

func fixFieldType(row map[string]any, field metadata.Field) {
	if v, o := row[field.Name]; o {
		switch field.DataType {
		case "integer", "int", "bigint":
			switch v := v.(type) {
			case uint, uint8, uint16, uint32, uint64, int, int8, int16, int32, int64, float32, float64:
				row[field.Name] = v
			default:
				row[field.Name], _ = strconv.ParseInt(fmt.Sprintf("%v", v), 10, 64)
			}
		case "boolean", "bool":
			t := fmt.Sprintf("%v", v)
			if contains([]string{"1", "true"}, t) {
				row[field.Name] = true
			} else {
				row[field.Name] = false
			}
		default:
			row[field.Name] = v

		}
	}
}

func connect(srcCon, destCon metadata.DataSource) error {
	var err error
	if srcCon == nil {
		return errors.New("No source connection")
	}
	if destCon == nil {
		return errors.New("No destination connection")
	}
	srcCon, err = srcCon.Connect()
	if err != nil {
		return err
	}
	_, err = destCon.Connect()
	return err
}

func contains[T comparable](s []T, v T) bool {
	for _, vv := range s {
		if vv == v {
			return true
		}
	}
	return false
}
