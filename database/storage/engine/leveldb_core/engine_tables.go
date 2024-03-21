package leveldb_core

func (engine *engine) Tables() (table_names map[string]int32) {
	table_names = make(map[string]int32, len(engine.tables))
	for _, table := range engine.tables {
		table_names[table.info.name] = table.id
	}
	return
}
