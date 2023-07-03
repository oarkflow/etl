package etl

import (
	"fmt"
	"github.com/oarkflow/pkg/str"
	"strconv"
	"strings"

	"github.com/oarkflow/errors"
	"github.com/oarkflow/metadata"
	"github.com/oarkflow/pkg/rule"
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
	Name          string
	Type          string
	KeyField      string
	ValueField    string
	DataTypeField string
	ExcludeFields []string
	IncludeFields []string
	ExtraValues   map[string]any
	KeyValueTable bool
	StoreDataType bool
}

type Config struct {
	RowLimit    int64
	BatchSize   int
	SkipTables  []string
	CloneTables []string
	CloneSource bool
	Persist     bool
}

type Page struct {
	Last    bool
	Offset  int64
	Limit   int
	Filters map[string]any
}

type ETL struct {
	srcCon       metadata.DataSource
	src          Source
	filters      []*rule.Rule
	transformers []Transformer
	destCon      metadata.DataSource
	dest         Destination
	cfg          Config
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
		cfg: config,
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

func (e *ETL) AddFilters(rules ...*rule.Rule) *ETL {
	if len(rules) > 0 {
		e.filters = append(e.filters, rules...)
	}
	return e
}

func (e *ETL) AddTransformer(transformer ...Transformer) *ETL {
	if len(transformer) > 0 {
		e.transformers = append(e.transformers, transformer...)
	}
	return e
}

func (e *ETL) ProcessPayload(payload []map[string]any) ([]map[string]any, error) {
	return e.process(0, payload)
}

func (e *ETL) process(batch int64, data []map[string]any) ([]map[string]any, error) {
	var err error
	var failedData, payload []map[string]any
	for _, filter := range e.filters {
		d, err := filter.Apply(data)
		if err != nil {
			return nil, err
		}
		data = d.([]map[string]any)
	}
	var destFields []metadata.Field
	if e.destCon != nil {
		destFields, err = e.destCon.GetFields(e.dest.Name)
		if err != nil {
			if err != nil {
				return nil, errors.NewE(err, fmt.Sprintf("Unable to get field list for %s", e.dest.Name), "ETLTransform")
			}
		}
	}

	for _, row := range data {
		for field, val := range row {
			lowerField := strings.ToLower(field)
			row[lowerField] = val
			if lowerField != field {
				delete(row, field)
			}
		}
		err = e.transform(row)
		if err != nil {
			return failedData, err
		}
		if e.destCon != nil {
			for _, field := range destFields {
				fixFieldType(row, field)
			}
		}
		if e.dest.KeyValueTable {
			keyValueData, err := e.processKeyValueTable(row)
			if err != nil {
				return nil, err
			}
			if len(keyValueData) > 0 {
				payload = append(payload, keyValueData...)
			}
		}
	}
	if !e.dest.KeyValueTable {
		return e.storeData(batch, data)
	}
	if len(payload) > 0 {
		return e.storeData(batch, payload)
	}
	return nil, nil
}

func (e *ETL) storeData(batch int64, data []map[string]any) ([]map[string]any, error) {
	var err error
	var failedData []map[string]any
	if len(data) > 0 {
		err = e.destCon.Store(e.dest.Name, data)
		if err != nil {
			panic(err)
			if !errors.Is(err, gorm.ErrDuplicatedKey) {
				failedData = append(failedData, data...)
			} else {
				return failedData, err
			}
		}
	}
	if len(failedData) > 0 {
		return e.processFailedData(map[int64][]map[string]any{
			batch: failedData,
		})
	}
	return failedData, nil
}

func (e *ETL) processKeyValueTable(row map[string]any) ([]map[string]any, error) {
	if e.dest.KeyField == "" {
		e.dest.KeyField = "key"
	}
	if e.dest.ValueField == "" {
		e.dest.ValueField = "value"
	}
	if e.dest.StoreDataType && e.dest.DataTypeField == "" {
		e.dest.DataTypeField = "value_type"
	}
	var rows []map[string]any
	srcFields, err := e.srcCon.GetFields(e.src.Name)
	if err != nil {
		return nil, err
	}
	for key, val := range row {
		data := make(map[string]any)
		for _, f := range e.dest.IncludeFields {
			if v, k := row[f]; k {
				data[f] = v
			}
		}
		for k, v := range e.dest.ExtraValues {
			data[k] = v
		}
		data[e.dest.KeyField] = key
		strVal := fmt.Sprintf("%v", val)
		if val == nil {
			data[e.dest.ValueField] = nil
		} else {
			data[e.dest.ValueField] = strVal
		}
		for _, f := range srcFields {
			if e.dest.StoreDataType && strings.ToLower(f.Name) == strings.ToLower(key) {
				data[e.dest.DataTypeField] = strings.ToLower(e.destCon.GetDataTypeMap(f.DataType))
			}
			switch f.DataType {
			case "tinyint":
				if strVal == "0" {
					data[e.dest.ValueField] = false
				} else if strVal == "1" {
					data[e.dest.ValueField] = true
				}
			}
		}
		if len(e.dest.ExcludeFields) > 0 && !str.Contains(e.dest.ExcludeFields, key) {
			delete(data, key)
		}
		if _, ok := data[e.dest.DataTypeField]; ok {
			rows = append(rows, data)
		}
	}
	return rows, nil
}

func (e *ETL) processFailedData(payload map[int64][]map[string]any) ([]map[string]any, error) {
	var failedData []map[string]any
	payloadLen := 0
	failedDataLen := 0
	for batch, d := range payload {
		for _, data := range d {
			payloadLen++
			err := e.destCon.Store(e.dest.Name, data)
			if err != nil {
				failedDataLen++
				if !errors.Is(err, gorm.ErrDuplicatedKey) {
					failedData = append(failedData, data)
				} else {
					return nil, err
				}
			}
		}
		fmt.Println("Processed...", payloadLen-failedDataLen, " failed records in", e.src.Name, " out of ", payloadLen, "in batch ", batch)
	}
	return failedData, nil
}

func (e *ETL) Process(filter ...map[string]any) (map[int64][]map[string]any, error) {
	failedData := make(map[int64][]map[string]any)
	if e.dest.Name == "" {
		e.dest.Name = e.src.Name
	}
	if e.cfg.CloneSource {
		err := metadata.CloneTable(e.srcCon, e.destCon, e.src.Name, e.dest.Name)
		if err != nil {
			return nil, err
		}
	}
	var totalData, failedRows int
	page := &Page{Limit: e.cfg.BatchSize}
	if len(filter) > 0 {
		page.Filters = filter[0]
	}
	fmt.Println("Processing migration for", e.src.Name, "to", e.dest.Name)
	for !page.Last {
		offset := page.Offset
		data, err := e.getData(page)
		totalData += len(data)
		if err != nil {
			return nil, errors.NewE(err, fmt.Sprintf("Unable to get data for %s", e.src.Name), "ETLTransform")
		}
		failed, err := e.process(offset, data)
		if err != nil {
			return nil, err
		}
		failedRows += len(failed)
		failedData[offset] = failed
	}
	fmt.Println("Processed...", totalData-failedRows, "records of", totalData, "in", e.src.Name)
	return failedData, nil
}

func (e *ETL) getData(page *Page) ([]map[string]any, error) {
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
	if len(page.Filters) > 0 {
		var condition []string
		for k, v := range page.Filters {
			filter[k] = v
			condition = append(condition, fmt.Sprintf("%s=@%s", k, k))
		}
		sql += " WHERE " + strings.Join(condition, ", ")
	}
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
	if e.cfg.RowLimit > 0 {
		sql += fmt.Sprintf(" LIMIT %d", e.cfg.RowLimit)
		return e.srcCon.GetRawCollection(sql, filter)
	}
	sql += fmt.Sprintf(" LIMIT %d, %d", page.Offset, page.Limit)
	data, err := e.srcCon.GetRawCollection(sql, filter)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		page.Last = true
	}
	page.Offset += int64(page.Limit)
	return data, nil
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
	if config.CloneSource {
		for _, table := range tables {
			err = metadata.CloneTable(srcCon, destCon, table, table)
			if err != nil {
				return err
			}
		}
	}

	for _, src := range tables {
		etl := New(Config{
			RowLimit:  config.RowLimit,
			Persist:   config.Persist,
			BatchSize: config.BatchSize,
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
			} else if contains([]string{"0", "false"}, t) {
				row[field.Name] = false
			} else {
				row[field.Name] = nil
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
