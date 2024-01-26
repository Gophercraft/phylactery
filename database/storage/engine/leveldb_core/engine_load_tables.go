package leveldb_core

import (
	"encoding/binary"
)

func (engine *engine) load_table(table_id int32) (err error) {
	var table_info_key [5]byte
	var table_info_value []byte
	binary.LittleEndian.PutUint32(table_info_key[0:4], uint32(table_id))
	table_info_key[4] = uint8(key_type_table_info)
	table := new(table)
	table.id = table_id
	table_info_value, err = engine.get(table_info_key[:])
	if err != nil {
		return
	}
	err = decode_table_info(table_info_value, &table.info)
	if err != nil {
		return err
	}
	engine.tables[int(table_id)] = table
	return nil
}

func (engine *engine) load_tables() (err error) {
	// See how many tables exist
	var num_tables_key [5]byte
	var reserved_table_ID int32 = -1
	binary.LittleEndian.PutUint32(num_tables_key[0:4], uint32(reserved_table_ID))
	num_tables_key[4] = uint8(key_type_num_tables)

	var num_tables_bytes []byte
	num_tables_bytes, err = engine.get(num_tables_key[:])
	if err != nil {
		// It's okay if there isn't a number for this yet
		return nil
	}
	num_tables := int32(binary.LittleEndian.Uint32(num_tables_bytes))
	// Open all of them
	engine.tables = make([]*table, num_tables)

	for table_id := int32(0); table_id < int32(num_tables); table_id++ {
		if err = engine.load_table(table_id); err != nil {
			return
		}
	}
	return err
}
