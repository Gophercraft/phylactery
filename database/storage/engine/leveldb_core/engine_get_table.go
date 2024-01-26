package leveldb_core

import "fmt"

func (engine *engine) get_table(table_id int32) (table *table, err error) {
	// Assert exclusive access
	engine.guard_tables.Lock()
	defer engine.guard_tables.Unlock()
	// See if table is already instantiated.
	if int(table_id) >= len(engine.tables) {
		return nil, fmt.Errorf("cannot find table id %d", table_id)
	}

	return engine.tables[table_id], nil
}
