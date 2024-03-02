package migrate

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/oarkflow/metadata"
	"github.com/oarkflow/pkg/evaluate"
	"gopkg.in/yaml.v3"

	"github.com/oarkflow/etl"
	"github.com/oarkflow/etl/mapper"
)

// TableConfig contains the configuration for a single table migration.
type TableConfig struct {
	OldName              string              `json:"old_name" yaml:"old_name"`
	NewName              string              `json:"new_name" yaml:"new_name"`
	QueryName            string              `yaml:"query_name" json:"query_name"`
	Query                string              `yaml:"query" json:"query"`
	QueryIdentifierField string              `yaml:"query_identifier_field" json:"query_identifier_field"`
	Migrate              bool                `json:"migrate" yaml:"migrate"`
	CloneSource          bool                `json:"clone_source" yaml:"clone_source"`
	BatchSize            int                 `json:"batch_size" yaml:"batch_size"`
	SkipStoreError       bool                `json:"skip_store_error" yaml:"skip_store_error"`
	TruncateDestination  bool                `json:"truncate_destination" yaml:"truncate_destination"`
	KeepUnmatchedFields  bool                `json:"keep_unmatched_fields" yaml:"keep_unmatched_fields"`
	ExcludeFields        []string            `json:"exclude_fields" yaml:"exclude_fields"`
	IncludeFields        []string            `json:"include_fields" yaml:"include_fields"`
	ExtraValues          map[string]any      `json:"extra_values" yaml:"extra_values"`
	KeyValueTable        bool                `json:"key_value_table" yaml:"key_value_table"`
	StoreDataType        bool                `json:"store_data_type" yaml:"store_data_type"`
	UpdateSequence       bool                `yaml:"update_sequence" json:"update_sequence"`
	Update               bool                `json:"update" yaml:"update"`
	Mapping              map[string]string   `json:"mapping" yaml:"mapping"`
	MultipleMapping      []map[string]string `json:"multiple_mapping" yaml:"multiple_mapping"`
	Lookups              string              `json:"lookups" yaml:"lookups"`
	UseLastInsertedID    bool                `yaml:"use_last_inserted_id" json:"use_last_inserted_id"`
}

func FromYaml(srcConfig, dstConfig metadata.Config, tableBytes []byte) error {
	var tableList []TableConfig
	err := yaml.Unmarshal(tableBytes, &tableList)
	if err != nil {
		return err
	}
	return Data(srcConfig, dstConfig, tableList)
}

func FromJson(srcConfig, dstConfig metadata.Config, tableBytes []byte) error {
	var tableList []TableConfig
	err := json.Unmarshal(tableBytes, &tableList)
	if err != nil {
		return err
	}
	return Data(srcConfig, dstConfig, tableList)
}

func Data(srcConfig, dstConfig metadata.Config, tableList []TableConfig) error {
	source := metadata.New(srcConfig)
	destination := metadata.New(dstConfig)

	for _, tableConfig := range tableList {
		if tableConfig.Migrate {
			if tableConfig.Query != "" {
				connector, err := source.Connect()
				if err != nil {
					return err
				}
				dataList, err := connector.GetRawCollection(tableConfig.Query)
				if err != nil {
					return err
				}
				var allSettings []map[string]any
				for _, data := range dataList {
					mapping := make(map[string]any)
					var commonMapping []map[string]any
					for _, mp := range tableConfig.MultipleMapping {
						tmp := make(map[string]any)
						for k, v := range mp {
							p, _ := evaluate.Parse(v, true)
							pr := evaluate.NewEvalParams(data)
							val, err := p.Eval(pr)
							if err == nil {
								tmp[k] = val
							} else {
								tmp[k] = v
							}
						}
						commonMapping = append(commonMapping, tmp)
					}
					for k, v := range tableConfig.Mapping {
						p, _ := evaluate.Parse(v, true)
						pr := evaluate.NewEvalParams(data)
						val, err := p.Eval(pr)
						if err == nil {
							mapping[k] = val
						} else {
							mapping[k] = v
						}
					}
					if len(commonMapping) > 0 {
						for _, cMap := range commonMapping {
							dataToMap := make(map[string]any)
							for k, v := range cMap {
								dataToMap[k] = v
							}
							for k, v := range mapping {
								dataToMap[k] = v
							}
							allSettings = append(allSettings, dataToMap)
						}
					} else {
						allSettings = append(allSettings, mapping)
					}

				}
				// insert all the settings into tableConfig.NewName
				dConnector, err := destination.Connect()
				if err != nil {
					return err
				}
				err = dConnector.StoreInBatches(tableConfig.NewName, allSettings, 1000)
				if err != nil {
					return err
				}
				fmt.Printf("Inserted %v the data into %s\n", len(allSettings), tableConfig.NewName)
			} else {
				err := tableMigration(source, destination, tableConfig)
				if err != nil {
					return err
				}
			}
		} else if tableConfig.OldName == "nil" {
			fmt.Printf("Creating new entry for %s\n", tableConfig.NewName)
			connector, err := destination.Connect()
			if err != nil {
				return err
			}
			mapping := make(map[string]any)
			for k, v := range tableConfig.Mapping {
				p, _ := evaluate.Parse(v, true)
				pr := evaluate.NewEvalParams(make(map[string]interface{}))
				val, err := p.Eval(pr)
				if err == nil {
					mapping[k] = val
				}
			}
			err = connector.Store(tableConfig.NewName, mapping)
			if err != nil {
				return err
			}
		}
		switch dstConfig.Driver {
		case "postgres", "psql", "postgresql":
			if tableConfig.UpdateSequence {
				// update the sequence counter for the table
				// get the primary key for a table
				connector, err := destination.Connect()
				if err != nil {
					return err
				}
				// get the name of the sequence counter for the table
				sql := fmt.Sprintf(
					` SELECT column_name, table_name,
            substring(column_default from '''([^'']+)''') AS sequence_name
	      	FROM information_schema.columns
      		WHERE column_default LIKE 'nextval%%' AND table_name = '%s';`, tableConfig.NewName)
				seqResult, err := connector.GetRawCollection(sql)
				if err != nil {
					return err
				}
				// get the column_name, table_name, and sequence_name from seqResult
				var columnName, tableName, sequenceName string
				for _, row := range seqResult {
					columnName = row["column_name"].(string)
					tableName = row["table_name"].(string)
					sequenceName = row["sequence_name"].(string)
				}
				// update the sequence counter for the table
				sql2 := fmt.Sprintf("SELECT setval('%s', COALESCE((SELECT MAX(%s)+1 FROM %s), 1), false);", sequenceName, columnName, tableName)
				err = connector.Exec(sql2)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func getLookups(source, destination metadata.DataSource, lookup string) (map[string]any, error) {
	if lookup == "" {
		return nil, nil
	}
	parts := strings.Split(lookup, ".")
	var toLookup string
	var useSource bool
	if len(parts) != 2 {
		useSource = true
		toLookup = parts[0]
	} else {
		toLookup = parts[1]
		switch parts[0] {
		case "source":
			useSource = true
		case "destination":
			useSource = false
		default:
			return nil, fmt.Errorf("invalid lookup %s as %s", toLookup, parts[0])
		}
	}
	var connector metadata.DataSource
	var err error
	if useSource {
		connector, err = source.Connect()
		if err != nil {
			return nil, err
		}
	} else {
		connector, err = destination.Connect()
		if err != nil {
			return nil, err
		}
	}
	sql := fmt.Sprintf("SELECT * FROM %s", lookup)
	lookups, err := connector.GetRawCollection(sql)
	if err != nil {
		return nil, err
	}
	return map[string]any{toLookup: lookups}, nil
}

// tableMigration migrates a single table from the source to the destination using the provided
// table configuration.
func tableMigration(source, destination metadata.DataSource, tableConfig TableConfig) error {
	lookups, err := getLookups(source, destination, tableConfig.Lookups)
	if err != nil {
		return err
	}
	if tableConfig.BatchSize == 0 {
		tableConfig.BatchSize = 500
	}
	instance := etl.New(etl.Config{
		CloneSource:         tableConfig.CloneSource,
		BatchSize:           tableConfig.BatchSize,
		SkipStoreError:      tableConfig.SkipStoreError,
		UseLastInsertedID:   tableConfig.UseLastInsertedID,
		TruncateDestination: tableConfig.TruncateDestination,
	})
	mp := mapper.New(&mapper.Config{
		FieldMaps:           tableConfig.Mapping,
		KeepUnmatchedFields: tableConfig.KeepUnmatchedFields,
		Lookups:             lookups,
	})

	instance.AddSource(source, etl.Source{Name: tableConfig.OldName})
	instance.AddTransformer(mp)
	instance.AddDestination(destination, etl.Destination{
		Name:          tableConfig.NewName,
		ExcludeFields: tableConfig.ExcludeFields,
		IncludeFields: tableConfig.IncludeFields,
		ExtraValues:   tableConfig.ExtraValues,
		KeyValueTable: tableConfig.KeyValueTable,
		StoreDataType: tableConfig.StoreDataType,
	})

	_, err = instance.Process()
	return err
}
