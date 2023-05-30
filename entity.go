package etl

type Relation struct {
	etl             *ETL
	identifierField string
}

type Entity struct {
	etl             *ETL
	relations       []*Relation
	identifierField string
	processEntity   bool
}

func NewEntity(etl *ETL, identifierField string, processEntity bool) *Entity {
	return &Entity{
		etl:             etl,
		identifierField: identifierField,
		processEntity:   processEntity,
	}
}

func (e *Entity) AddRelation(etl *ETL, identifierField string) {
	e.relations = append(e.relations, &Relation{etl: etl, identifierField: identifierField})
}

func (e *Entity) Process(id any) ([]map[string]any, error) {
	err := connect(e.etl.srcCon, e.etl.destCon)
	if err != nil {
		return nil, err
	}
	filter := map[string]any{
		e.identifierField: id,
	}
	if e.processEntity {
		_, err = e.etl.Process(filter)
		if err != nil {
			return nil, err
		}
	}
	for _, relation := range e.relations {
		_, err = relation.etl.Process(map[string]any{
			relation.identifierField: id,
		})
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}
