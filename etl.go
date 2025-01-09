package etl

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gosimple/slug"
	"github.com/oarkflow/log"
	"github.com/oarkflow/pkg/evaluate"
	"github.com/oarkflow/pkg/str"

	"github.com/oarkflow/errors"
	"github.com/oarkflow/metadata"
	"github.com/oarkflow/pkg/rule"
)

func init() {
	evaluate.AddCustomOperator("slug", func(ctx evaluate.EvalContext) (interface{}, error) {
		if err := ctx.CheckArgCount(1); err != nil {
			return "", err
		}
		st, err := ctx.Arg(0)
		if err != nil {
			return "", err
		}
		return slug.Make(fmt.Sprintf("%v", st)), nil
	})
}

type Data any

type Transformer interface {
	Name() string
	Transform(data Data) error
}

type Source struct {
	Name        string   `json:"name"`
	Query       string   `json:"query"`
	Type        string   `json:"type"`
	Identifiers []string `json:"identifiers"`
}

type Destination struct {
	Name                 string         `json:"name"`
	Type                 string         `json:"type"`
	KeyField             string         `json:"key_field"`
	ValueField           string         `json:"value_field"`
	DataTypeField        string         `json:"data_type_field"`
	ExcludeFields        []string       `json:"exclude_fields"`
	IncludeFields        []string       `json:"include_fields"`
	Transformers         []Transformer  `json:"transformers"`
	ExtraValues          map[string]any `json:"extra_values"`
	MultipleDestinations []Destination  `json:"multiple_destinations"`
	KeyValueTable        bool           `json:"key_value_table"`
	StoreDataType        bool           `json:"store_data_type"`
}

type Config struct {
	RowLimit            int64    `json:"row_limit"`
	BatchSize           int      `json:"batch_size"`
	SkipTables          []string `json:"skip_tables"`
	CloneTables         []string `json:"clone_tables"`
	CloneSource         bool     `json:"clone_source"`
	TruncateDestination bool     `json:"truncate_destination"`
	SkipStoreError      bool     `json:"skip_store_error"`
	UseLastInsertedID   bool     `json:"use_last_inserted_id"`
}

type Page struct {
	Last    bool           `json:"last"`
	Offset  int64          `json:"offset"`
	Limit   int            `json:"limit"`
	Filters map[string]any `json:"filters"`
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
			return nil, errors.NewE(err, fmt.Sprintf("Unable to get field list for %s", e.dest.Name), "ETLTransform")
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
		} else {
			payload = append(payload, row)
		}
	}
	if !e.dest.KeyValueTable && e.destCon != nil && len(data) > 0 {
		rs, err := e.storeData(batch, data)
		if err != nil {
			return rs, err
		}
		return rs, err
	}

	if len(payload) > 0 && e.destCon != nil {
		rs, err := e.storeData(batch, payload)
		if err != nil {
			return rs, err
		}
		return rs, err
	}
	return payload, nil
}

func (e *ETL) storeData(batch int64, data []map[string]any) ([]map[string]any, error) {
	var err error
	var failedData []map[string]any
	if len(data) > 0 {
		err = e.destCon.Store(e.dest.Name, data)
		if err != nil {
			if !e.cfg.SkipStoreError {
				errMsg := err.Error()
				if strings.Contains("duplicated key not allowed", errMsg) {
					return failedData, err
				}
				if strings.Contains(errMsg, "duplicate key value violates unique") {
					return failedData, err
				}
			}

			failedData = append(failedData, data...)
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
			src := fmt.Sprintf("%v", v)
			if strings.HasPrefix(src, "{{") {
				p, _ := evaluate.Parse(src, true)
				pr := evaluate.NewEvalParams(data)
				d, err := p.Eval(pr)
				if err == nil {
					data[k] = d
				}
			} else if val, ok := data[src]; ok {
				data[k] = val
			} else {
				data[k] = v
			}
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
			switch data[e.dest.DataTypeField] {
			case "boolean":
				if strVal == "0" {
					data[e.dest.ValueField] = "false"
				} else if strVal == "1" {
					data[e.dest.ValueField] = "true"
				}
			}
		}
		if len(e.dest.ExcludeFields) > 0 {
			for ka, _ := range data {
				if str.Contains(e.dest.ExcludeFields, ka) {
					delete(data, ka)
				}
			}
		}

		if len(e.dest.ExcludeFields) > 0 {
			if !str.Contains(e.dest.ExcludeFields, fmt.Sprintf("%v", data[e.dest.KeyField])) {
				rows = append(rows, data)
			}
		} else {
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
				log.Error().Err(err).Msgf("Error on storing data to %s", e.dest.Name)
				failedDataLen++
				if !e.cfg.SkipStoreError {
					errMsg := err.Error()
					if strings.Contains("duplicated key not allowed", errMsg) {
						return failedData, err
					}
					if strings.Contains(errMsg, "duplicate key value violates unique") {
						return failedData, err
					}
				}
				failedData = append(failedData, data)
			}
		}
		processedData := payloadLen - failedDataLen
		if processedData > 0 {
			fmt.Println("Processed Failed Data...", processedData, " failed records in", e.src.Name, " out of ", payloadLen, "in batch ", batch)
		} else {
			fmt.Println("Didn't process data")
		}
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
	err := connect(e.srcCon, e.destCon)
	if err != nil {
		return nil, err
	}
	if e.cfg.TruncateDestination && e.destCon != nil {
		err := e.destCon.Exec("TRUNCATE TABLE " + e.dest.Name)
		if err != nil {
			err = e.destCon.Exec("DELETE FROM " + e.dest.Name)
			if err != nil {
				return nil, errors.NewE(err, e.dest.Name, "ETLMigration")
			}
		}
	}
	fmt.Println(fmt.Sprintf("Processing migration for %s.%s.%s to %s.%s.%s", e.srcCon.Config().Driver, e.srcCon.Config().Database, e.src.Name, e.destCon.Config().Driver, e.destCon.Config().Database, e.dest.Name))
	for !page.Last {
		offset := page.Offset
		data, err := e.GetData(page)
		dataLen := len(data)
		totalData += dataLen
		if err != nil {
			return nil, errors.NewE(err, fmt.Sprintf("Unable to get data for %s", e.src.Name), "ETLTransform")
		}
		failed, err := e.process(offset, data)
		if err != nil {
			return nil, err
		}
		data = nil
		failedRows += len(failed)
		failedData[offset] = failed
		fmt.Println(fmt.Sprintf("Processed %d records from %s.%s.%s to %s.%s.%s at %v", offset, e.srcCon.Config().Driver, e.srcCon.Config().Database, e.src.Name, e.destCon.Config().Driver, e.destCon.Config().Database, e.dest.Name, time.Now()))
	}
	processedRecords := totalData - failedRows
	if processedRecords > 0 {
		fmt.Println(fmt.Sprintf("Processed %d records from %s.%s.%s to %s.%s.%s at %v", processedRecords, e.srcCon.Config().Driver, e.srcCon.Config().Database, e.src.Name, e.destCon.Config().Driver, e.destCon.Config().Database, e.dest.Name, time.Now()))
	}

	if len(e.dest.MultipleDestinations) > 0 {
		e.processMultipleDestinations()
	}
	return failedData, nil
}

func (e *ETL) GetData(page *Page) ([]map[string]any, error) {
	if e.src.Name == "" {
		return nil, errors.New("source not defined")
	}
	fields, _ := e.srcCon.GetFields(e.src.Name)
	filter := make(map[string]any)
	migrationInfoFile, err := os.ReadFile(fmt.Sprintf("%s.json", e.src.Name))
	sql := fmt.Sprintf("SELECT * FROM %s", e.src.Name)
	var migrationInfo MigrationInfo
	if err == nil {
		json.Unmarshal(migrationInfoFile, &migrationInfo)
	}
	if len(page.Filters) > 0 {
		var condition []string
		for k, v := range page.Filters {
			filter[k] = v
			condition = append(condition, fmt.Sprintf("%s=@%s", k, k))
		}
		sql += " WHERE " + strings.Join(condition, "AND ")
		if migrationInfo.LastInsertedID != nil && e.cfg.UseLastInsertedID {
			id, _ := strconv.ParseInt(fmt.Sprintf("%v", migrationInfo.LastInsertedID), 10, 64)
			if id != 0 {
				sql += " AND " + fmt.Sprintf("%s > %d", migrationInfo.PrimaryKey, id)
			}
		}
	} else {
		if migrationInfo.LastInsertedID != nil && e.cfg.UseLastInsertedID {
			id, _ := strconv.ParseInt(fmt.Sprintf("%v", migrationInfo.LastInsertedID), 10, 64)
			if id != 0 {
				sql += " WHERE " + fmt.Sprintf("%s > %d", migrationInfo.PrimaryKey, id)
			}
		}
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

func (e *ETL) processMultipleDestinations() error {
	for _, dest := range e.dest.MultipleDestinations {
		etl := New(e.cfg)
		etl.AddSource(e.srcCon, e.src)
		if len(dest.Transformers) > 0 {
			etl.AddTransformer(dest.Transformers...)
		}
		etl.AddDestination(e.destCon, dest)
		_, err := etl.Process()
		if err != nil {
			return err
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
			RowLimit:          config.RowLimit,
			BatchSize:         config.BatchSize,
			SkipTables:        config.SkipTables,
			CloneTables:       config.CloneTables,
			CloneSource:       config.CloneSource,
			SkipStoreError:    config.SkipStoreError,
			UseLastInsertedID: config.UseLastInsertedID,
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
