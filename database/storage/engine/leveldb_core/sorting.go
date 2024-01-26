package leveldb_core

import (
	"fmt"

	"github.com/Gophercraft/phylactery/database/storage"
)

// import (
// 	"encoding/binary"
// 	"fmt"

// 	"github.com/Gophercraft/phylactery/database/storage"
// 	"github.com/Gophercraft/phylactery/database/storage/record"
// 	"github.com/Gophercraft/phylactery/database/storage/sort64"
// )

type sorting_comparator_func func(a, b any) bool

func sorting_comparator_uint8(a, b any) bool {
	return a.(uint8) >= b.(uint8)
}
func sorting_comparator_uint16(a, b any) bool {
	return a.(uint16) >= b.(uint16)
}
func sorting_comparator_uint32(a, b any) bool {
	return a.(uint32) >= b.(uint32)
}
func sorting_comparator_uint64(a, b any) bool {
	return a.(uint64) >= b.(uint64)
}

func sorting_comparator_int8(a, b any) bool {
	return a.(int8) >= b.(int8)
}
func sorting_comparator_int16(a, b any) bool {
	return a.(int16) >= b.(int16)
}
func sorting_comparator_int32(a, b any) bool {
	return a.(int32) >= b.(int32)
}
func sorting_comparator_int64(a, b any) bool {
	return a.(int64) >= b.(int64)
}

func sorting_comparator_float32(a, b any) bool {
	return a.(float32) >= b.(float32)
}
func sorting_comparator_float64(a, b any) bool {
	return a.(float64) >= b.(float64)
}

func sorting_comparator_string(a, b any) bool {
	return a.(string) >= b.(string)
}

// type table_column_index_sorter struct {
// 	column_index      int
// 	column_tag        uint32
// 	table             *table
// 	sorting_comparator sorting_comparator_func
// }

// func (sorter *table_column_index_sorter) Len() int64 {
// 	sorter.table.lock_info()
// 	rows := int64(sorter.table.info.rows)
// 	sorter.table.unlock_info()
// 	return rows
// }

// func (sorter *table_column_index_sorter) Swap(i, j int64) {
// 	// Build index keys
// 	var i_key [4 + 1 + 4 + 8]byte
// 	var j_key [4 + 1 + 4 + 8]byte
// 	binary.LittleEndian.PutUint32(i_key[0:4], uint32(sorter.table.id))
// 	binary.LittleEndian.PutUint32(j_key[0:4], uint32(sorter.table.id))
// 	i_key[4] = uint8(key_type_table_column_index)
// 	j_key[4] = uint8(key_type_table_column_index)
// 	binary.LittleEndian.PutUint32(i_key[5:9], uint32(sorter.column_tag))
// 	binary.LittleEndian.PutUint32(j_key[5:9], uint32(sorter.column_tag))
// 	binary.LittleEndian.PutUint64(i_key[9:17], uint64(i))
// 	binary.LittleEndian.PutUint64(j_key[9:17], uint64(j))

// 	// Get record IDs pointed to by i and j
// 	i_value, err := sorter.table.engine.get(i_key[:])
// 	if err != nil {
// 		panic(err)
// 	}
// 	j_value, err := sorter.table.engine.get(j_key[:])
// 	if err != nil {
// 		panic(err)
// 	}

// 	// Swap the record IDs
// 	sorter.table.engine.put(i_key[:], j_value)
// 	sorter.table.engine.put(j_key[:], i_value)
// }

// func (sorter *table_column_index_sorter) Less(i, j int64) bool {
// 	// Build index keys
// 	var i_key [4 + 1 + 4 + 8]byte
// 	var j_key [4 + 1 + 4 + 8]byte
// 	binary.LittleEndian.PutUint32(i_key[0:4], uint32(sorter.table.id))
// 	binary.LittleEndian.PutUint32(j_key[0:4], uint32(sorter.table.id))
// 	i_key[4] = uint8(key_type_table_column_index)
// 	j_key[4] = uint8(key_type_table_column_index)
// 	binary.LittleEndian.PutUint32(i_key[5:9], uint32(sorter.column_tag))
// 	binary.LittleEndian.PutUint32(j_key[5:9], uint32(sorter.column_tag))
// 	binary.LittleEndian.PutUint64(i_key[9:17], uint64(i))
// 	binary.LittleEndian.PutUint64(j_key[9:17], uint64(j))

// 	// Get record IDs pointed to by i and j
// 	i_value, err := sorter.table.engine.get(i_key[:])
// 	if err != nil {
// 		panic(err)
// 	}
// 	j_value, err := sorter.table.engine.get(j_key[:])
// 	if err != nil {
// 		panic(err)
// 	}
// 	i_record_ID := binary.LittleEndian.Uint64(i_value)
// 	j_record_ID := binary.LittleEndian.Uint64(j_value)

// 	// Lookup records by ID
// 	var i_record_key [13]byte
// 	var j_record_key [13]byte
// 	binary.LittleEndian.PutUint32(i_record_key[0:4], uint32(sorter.table.id))
// 	binary.LittleEndian.PutUint32(j_record_key[0:4], uint32(sorter.table.id))
// 	i_record_key[4] = uint8(key_type_table_record)
// 	j_record_key[4] = uint8(key_type_table_record)
// 	binary.LittleEndian.PutUint64(i_key[5:13], i_record_ID)
// 	binary.LittleEndian.PutUint64(j_key[5:13], j_record_ID)
// 	i_record_value, err := sorter.table.engine.get(i_record_key[:])
// 	if err != nil {
// 		panic(err)
// 	}
// 	j_record_value, err := sorter.table.engine.get(j_record_key[:])
// 	if err != nil {
// 		panic(err)
// 	}
// 	// Extract the columns we need to compare
// 	i_record, err := record.Unmarshal(&sorter.table.info.schema, i_record_value)
// 	if err != nil {
// 		panic(err)
// 	}
// 	j_record, err := record.Unmarshal(&sorter.table.info.schema, j_record_value)
// 	if err != nil {
// 		panic(err)
// 	}
// 	i_column := i_record[sorter.column_index]
// 	j_column := j_record[sorter.column_index]

// 	return sorter.sorting_comparator(j_column, i_column)
// }

func get_comparator_func(kind storage.TableSchemaColumnKind, size int32) sorting_comparator_func {
	switch kind {
	case storage.TableSchemaColumnUint:
		switch size {
		case 8:
			return sorting_comparator_uint8
		case 16:
			return sorting_comparator_uint16
		case 32:
			return sorting_comparator_uint32
		case 64:
			return sorting_comparator_uint64
		default:
			panic(fmt.Errorf("invalid uint size %d", size))
		}
	case storage.TableSchemaColumnInt:
		switch size {
		case 8:
			return sorting_comparator_int8
		case 16:
			return sorting_comparator_int16
		case 32:
			return sorting_comparator_int32
		case 64:
			return sorting_comparator_int64
		default:
			panic(fmt.Errorf("invalid int size %d", size))
		}
	case storage.TableSchemaColumnFloat:
		switch size {
		case 32:
			return sorting_comparator_float32
		case 64:
			return sorting_comparator_float64
		default:
			panic(fmt.Errorf("invalid float size %d", size))
		}
	case storage.TableSchemaColumnString:
		return sorting_comparator_string
	default:
		panic(fmt.Errorf("can't compare kind of %d", kind))
	}
}

// func (table *table) sort_indices() error {
// 	for column_index, column_schema := range table.info.schema.Columns {
// 		if column_schema.Index && !column_schema.Exclusive {
// 			var sorter table_column_index_sorter
// 			sorter.table = table
// 			sorter.column_index = column_index
// 			sorter.column_tag = column_schema.Tag
// 			sorter.sorting_comparator = get_comparator_func(column_schema.Kind, column_schema.Size)

// 			sort64.Sort(&sorter)
// 		}
// 	}

// 	return nil
// }
