package leveldb_core

import "encoding/binary"

func (engine *engine) put_table_info(table_id int32) error {
	table := engine.tables[int(table_id)]
	if table == nil {
		panic(table_id)
	}
	encoded_table_info, err := encode_table_info(&table.info)
	if err != nil {
		return err
	}
	var table_key_bytes [5]byte
	binary.LittleEndian.PutUint32(table_key_bytes[0:4], uint32(table_id))
	table_key_bytes[4] = uint8(key_type_table_info)
	return engine.put_sync(table_key_bytes[:], encoded_table_info)
}
