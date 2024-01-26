package leveldb_core

import (
	"encoding/binary"
)

func (engine *engine) CreateTable(name string) int32 {
	engine.guard_tables.Lock()
	defer engine.guard_tables.Unlock()
	for index, table := range engine.tables {
		// return handle to existing database
		if table.info.name == name {
			return int32(index)
		}
	}
	// table wasn't found, create one
	table_id := int32(len(engine.tables))
	// It's valid to have an empty table structure like this
	new_table := new(table)
	new_table.id = table_id
	new_table.info.name = name
	new_table.info.auto_increment_counters = make(table_counters)
	engine.tables = append(engine.tables, new_table)

	// Enter updated number of tables in key-value store
	var num_tables_key [5]byte
	// reserved empty table value (-1 sorted first)
	var null_table_ID int32 = -1
	binary.LittleEndian.PutUint32(num_tables_key[0:4], uint32(null_table_ID))
	num_tables_key[4] = uint8(key_type_num_tables)
	var num_tables_bytes [4]byte
	binary.LittleEndian.PutUint32(num_tables_bytes[:], uint32(len(engine.tables)))
	engine.put(num_tables_key[:], num_tables_bytes[:])

	engine.put_table_info(table_id)

	return table_id
}
