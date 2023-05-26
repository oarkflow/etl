package main

import (
	"encoding/json"
	"fmt"

	"github.com/oarkflow/metadata"
	"github.com/oarkflow/pkg/rule"

	"github.com/oarkflow/etl"
	"github.com/oarkflow/etl/mapper"
)

func main() {
	// migrateDB()
	// tableMigration()
	settingsTableMigration()
	// etlWithFilter()
}

func etlWithFilter() {
	data := `[{"code": "A000", "desc": "Cholera due to Vibrio cholerae 01, biovar cholerae"}, {"code": "A001", "desc": "Cholera due to Vibrio cholerae 01, biovar eltor"}, {"code": "A009", "desc": "Cholera, unspecified"}, {"code": "A0100", "desc": "Typhoid fever, unspecified"}, {"code": "A0101", "desc": "Typhoid meningitis"}, {"code": "A0102", "desc": "Typhoid fever with heart involvement"}, {"code": "A0103", "desc": "Typhoid pneumonia"}, {"code": "A0104", "desc": "Typhoid arthritis"}, {"code": "A0105", "desc": "Typhoid osteomyelitis"}]`
	var d []map[string]any
	json.Unmarshal([]byte(data), &d)
	r := rule.New()
	r.And(rule.NewCondition("code", rule.IN, []string{"A000", "A001"}))

	mapper := mapper.New(&mapper.Config{
		FieldMaps: map[string]string{
			"code": "cpt_code",
		},
		KeepUnmatchedFields: false,
	})

	e := etl.New()
	e.AddFilters(r)
	e.AddTransformer(mapper)
	fmt.Println(e.ProcessPayload(d))
}

func settingsTableMigration() {
	source, destination := conn()
	instance := etl.New(etl.Config{CloneSource: false})
	instance.AddSource(source, etl.Source{Name: "tbl_work_item"})
	instance.AddDestination(destination, etl.Destination{
		Name:          "work_item_settings",
		Type:          "table",
		ExcludeFields: []string{"facility_id", "work_item_type_id", "work_item_uid"},
		KeyValueTable: true,
		StoreDataType: true,
	})
	_, err := instance.Process()
	if err != nil {
		panic(err)
	}
}

func tableMigration() {
	mapper := mapper.New(&mapper.Config{
		FieldMaps: map[string]string{
			"cdi_reason_id": "cdi_id",
			"area":          "cdi_area",
			"status":        "{{'ACTIVE'}}",
			"created_at":    "{{now()}}",
		},
		KeepUnmatchedFields: false,
	})
	source, destination := conn()
	instance := etl.New(etl.Config{CloneSource: false})
	instance.AddSource(source, etl.Source{Name: "cdi_reason"})
	instance.AddTransformer(mapper)
	instance.AddDestination(destination, etl.Destination{Name: "tmp_cdi_reason"})
	_, err := instance.Process()
	if err != nil {
		panic(err)
	}
}

func migrateDB() {
	source, destination := conn()
	err := etl.MigrateDB(source, destination, etl.Config{
		CloneSource: true,
		Persist:     false,
		CloneTables: []string{"cdi_reason"},
	})
	if err != nil {
		panic(err)
	}
}

func conn() (metadata.DataSource, metadata.DataSource) {
	cfg1 := metadata.Config{
		Host:          "localhost",
		Port:          3306,
		Driver:        "mysql",
		Username:      "root",
		Password:      "root",
		Database:      "cleardb",
		DisableLogger: true,
	}
	cfg := metadata.Config{
		Host:          "localhost",
		Port:          5432,
		Driver:        "postgresql",
		Username:      "postgres",
		Password:      "postgres",
		Database:      "clear",
		DisableLogger: true,
	}
	source := metadata.New(cfg1)
	destination := metadata.New(cfg)
	return source, destination
}
