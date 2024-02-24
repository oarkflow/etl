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
	// entityMigration()
	// settingsTableMigration()
	etlWithFilter()
	// testRawSqlWithMapFilter()
}

func testRawSqlWithMapFilter() {
	src, _ := conn()
	src.Connect()
	fmt.Println(src.GetRawCollection("SELECT * FROM tbl_user LIMIT 1", map[string]any{"user_email_address": "spbaniya@deerwalk.com"}))
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
	mapper := mapper.New(&mapper.Config{
		FieldMaps: map[string]string{
			"user_id": "{{lookupIn('users', 'user_id', [user_uid], 'user_id')}}",
		},
		KeepUnmatchedFields: true,
		Lookups: map[string][]map[string]any{
			"users": {
				{
					"user_id":            37,
					"user_email_address": "abc@example.com",
				},
				{
					"user_id":            33,
					"user_email_address": "abc1@example.com",
				},
				{
					"user_id":            21,
					"user_email_address": "abc2@example.com",
				},
				{
					"user_id":            35,
					"user_email_address": "abc2@example.com",
				},
			},
		},
	})
	source, destination := conn()
	instance := etl.New(etl.Config{CloneSource: false})
	instance.AddSource(source, etl.Source{Name: "tbl_user_setting"})
	instance.AddTransformer(mapper)
	instance.AddDestination(destination, etl.Destination{
		Name:          "work_item_settings",
		Type:          "table",
		ExcludeFields: []string{"facility_id", "work_item_type_id", "work_item_uid"},
		IncludeFields: []string{"work_item_id"},
		ExtraValues: map[string]any{
			"wi_setting_order_index": 1,
		},
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

func entityMigration() {
	source, destination := conn()

	e := etl.New(etl.Config{CloneSource: false})
	e.AddSource(source, etl.Source{Name: "tbl_work_item"})
	e.AddDestination(destination, etl.Destination{Name: "work_items"})

	mp := mapper.New(&mapper.Config{
		FieldMaps: map[string]string{
			"work_item_uid": "work_item_uid",
			"charge_type":   "charge_type",
			"code":          "code",
			"no_charge":     "no_charge",
		},
		KeepUnmatchedFields: false,
	})

	r1 := etl.New(etl.Config{CloneSource: true})
	r1.AddSource(source, etl.Source{Name: "tbl_work_item_em_level"})
	r1.AddTransformer(mp)
	r1.AddDestination(destination, etl.Destination{Name: "work_item_em_levels"})

	entity := etl.NewEntity(e, "work_item_uid", false)
	entity.AddRelation(r1, "work_item_uid")
	fmt.Println(entity.Process(29))
}

func migrateDB() {
	source, destination := conn()
	err := etl.MigrateDB(source, destination, etl.Config{
		CloneSource: true,
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
