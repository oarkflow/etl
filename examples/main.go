package main

import (
	"github.com/oarkflow/metadata"

	"github.com/oarkflow/etl"
)

func main() {
	migrateDB()
}

func etlProcess() {
	source, destination := conn()
	/*mapper := mapper.New(&mapper.Config{
		FieldMaps: map[string]string{
			"first_name": "name",
		},
		KeepUnmatchedFields: false,
	})*/
	/*concat := concat.New(&concat.Config{
		SourceFields:     []string{"first_name", "last_name"},
		DestinationField: "name",
		KeepSourceFields: false,
	})*/
	instance := etl.New()
	instance.AddSource(source, etl.Source{Name: "tbl_user"})
	// instance.AddTransformer(mapper)
	instance.AddDestination(destination, etl.Destination{})
	instance.CloneSource(true)
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
		// CloneTables: []string{"cdi_reason"},
	})
	if err != nil {
		panic(err)
	}
}

func conn() (metadata.DataSource, metadata.DataSource) {
	cfg1 := metadata.Config{
		Host:     "localhost",
		Port:     3307,
		Driver:   "mysql",
		Username: "root",
		Password: "root",
		Database: "cleardb",
	}
	cfg := metadata.Config{
		Host:     "localhost",
		Port:     5432,
		Driver:   "postgresql",
		Username: "postgres",
		Password: "postgres",
		Database: "clear",
	}
	source := metadata.New(cfg1)
	destination := metadata.New(cfg)
	return source, destination
}
