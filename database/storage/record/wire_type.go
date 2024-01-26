package record

import (
	"fmt"

	"github.com/Gophercraft/phylactery/database/storage"
)

type wire_type uint8

const (
	wire_type_u8 = iota
	wire_type_u16
	wire_type_u32
	wire_type_u64
	wire_type_i8
	wire_type_i16
	wire_type_i32
	wire_type_i64
	wire_type_f32
	wire_type_f64
	wire_type_bool
	wire_type_string
	wire_type_slice
	wire_type_structure
	wire_type_map
)

func get_wire_type(kind storage.TableSchemaColumnKind, size int32) (wire_type, error) {
	switch kind {
	case storage.TableSchemaColumnUint:
		switch size {
		case 8:
			return wire_type_u8, nil
		case 16:
			return wire_type_u16, nil
		case 32:
			return wire_type_u32, nil
		case 64:
			return wire_type_u64, nil
		default:
			return 0, fmt.Errorf("get_wire_type: invalid uint size %d", size)
		}
	case storage.TableSchemaColumnInt:
		switch size {
		case 8:
			return wire_type_i8, nil
		case 16:
			return wire_type_i16, nil
		case 32:
			return wire_type_i32, nil
		case 64:
			return wire_type_i64, nil
		default:
			return 0, fmt.Errorf("invalid int size %d", size)
		}
	case storage.TableSchemaColumnFloat:
		switch size {
		case 32:
			return wire_type_f32, nil
		case 64:
			return wire_type_f64, nil
		default:
			return 0, fmt.Errorf("invalid float size %d", size)
		}
	case storage.TableSchemaColumnBool:
		return wire_type_bool, nil
	case storage.TableSchemaColumnString:
		return wire_type_string, nil
	case storage.TableSchemaColumnStructure:
		return wire_type_structure, nil
	case storage.TableSchemaColumnSlice, storage.TableSchemaColumnArray:
		return wire_type_slice, nil
	default:
		return 0, fmt.Errorf("unknown kind %d", kind)
	}
}
