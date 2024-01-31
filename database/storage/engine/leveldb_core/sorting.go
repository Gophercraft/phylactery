package leveldb_core

import (
	"fmt"

	"github.com/Gophercraft/phylactery/database/storage"
)

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
