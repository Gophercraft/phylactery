package leveldb_core

type key_type uint8

const (
	// should never appear at runtime.
	key_type_null key_type = iota
	// used to store the number of tables per storage engine
	// [reserved table ID of -1 : int32, key_type, num_tables : uint32]
	key_type_num_tables
	// used to store the schematic information of a table, the number of rows
	// the row ID counter, and auto-increment field counters
	// [table ID : int32, key_type, table_info]
	key_type_table_info
	// used to store the actual record data. each record is identified by a unique 64-bit unsigned integer
	// called the record or row ID. Note that this ID is opaque to the public API of Phylactery.
	// [table ID : int32, key_type, row ID : uint64] -> [record data : any]
	key_type_table_record
	// A repeatable index is like exclusive index, except containing a row ID in the key
	// the row ID makes the key unique, while being able to have multiple keys with the same packed value pointing to several different row IDs
	// [table ID : int32, key_type, column tag : uint32, index_type, encoded value ] -> [row ID : uint64]
	key_type_table_column_repeatable_index
	// [table ID : int32, key_type, column tag : uint32, packed binary value : any] -> [row ID : uint64]
	key_type_table_column_exclusive_index
)
