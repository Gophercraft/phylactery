package leveldb_core

import (
	"encoding/binary"
	"reflect"

	"github.com/Gophercraft/phylactery/database/storage"
)

func make_exclusive_key(table_id int32, column_schema *storage.TableSchemaColumn, column_value any) []byte {
	exclusive_index_key := make([]byte, 9)
	binary.LittleEndian.PutUint32(exclusive_index_key[0:4], uint32(table_id))
	exclusive_index_key[4] = uint8(key_type_table_column_exclusive_index)
	binary.LittleEndian.PutUint32(exclusive_index_key[5:9], uint32(column_schema.Tag))
	// for simplicity use max size 64
	switch column_schema.Kind {
	case storage.TableSchemaColumnUint:
		var uintfield [8]byte
		binary.LittleEndian.PutUint64(uintfield[:], reflect.ValueOf(column_value).Uint())
		exclusive_index_key = append(exclusive_index_key, uintfield[:]...)
	case storage.TableSchemaColumnInt:
		var intfield [8]byte
		binary.LittleEndian.PutUint64(intfield[:], uint64(reflect.ValueOf(column_value).Int()))
		exclusive_index_key = append(exclusive_index_key, intfield[:]...)
	case storage.TableSchemaColumnString:
		exclusive_index_key = append(exclusive_index_key, []byte(column_value.(string))...)
	default:
		panic("field is not indexable")
	}
	return exclusive_index_key
}

func make_repeatable_key(table_id int32, column_schema *storage.TableSchemaColumn, record_ID uint64, column_value any) []byte {
	repeatable_index_key := make([]byte, 18)
	binary.LittleEndian.PutUint32(repeatable_index_key[0:4], uint32(table_id))
	repeatable_index_key[4] = uint8(key_type_table_column_exclusive_index)
	binary.LittleEndian.PutUint32(repeatable_index_key[5:9], uint32(column_schema.Tag))
	binary.LittleEndian.PutUint64(repeatable_index_key[9:17], uint64(record_ID))
	repeatable_index_key[17] = uint8(column_schema.Kind)
	// for simplicity use max size 64
	switch column_schema.Kind {
	case storage.TableSchemaColumnUint:
		var uintfield [8]byte
		binary.LittleEndian.PutUint64(uintfield[:], reflect.ValueOf(column_value).Uint())
		repeatable_index_key = append(repeatable_index_key, uintfield[:]...)
	case storage.TableSchemaColumnInt:
		var intfield [8]byte
		binary.LittleEndian.PutUint64(intfield[:], uint64(reflect.ValueOf(column_value).Int()))
		repeatable_index_key = append(repeatable_index_key, intfield[:]...)
	case storage.TableSchemaColumnString:
		repeatable_index_key = append(repeatable_index_key, []byte(column_value.(string))...)
	default:
		panic("field is not indexable")
	}
	return repeatable_index_key
}

func make_record_sector_key(table_id int32, record_id uint64) []byte {
	record_sector_key := make([]byte, 13)
	binary.LittleEndian.PutUint32(record_sector_key[0:4], uint32(table_id))
	record_sector_key[4] = uint8(key_type_table_record)
	binary.LittleEndian.PutUint64(record_sector_key[5:13], record_id)
	return record_sector_key
}
