package leveldb_core

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/Gophercraft/phylactery/database/storage"
)

type key_comparator struct {
}

func (key_comparator *key_comparator) Compare(a, b []byte) int {
	a_table := int32(binary.LittleEndian.Uint32(a[0:4]))
	b_table := int32(binary.LittleEndian.Uint32(b[0:4]))

	if a_table < b_table {
		return -1
	}

	if a_table > b_table {
		return 1
	}

	a_key := key_type(a[4])
	b_key := key_type(b[4])

	// The table's values are sorted into sectors based on key type
	if a_key < b_key {
		return -1
	}

	if a_key > b_key {
		return 1
	}

	// If equal determine how to sort
	switch a_key {
	case key_type_table_info:
		return 0
	case key_type_num_tables:
		return 0
	case key_type_table_column_exclusive_index:
		// Parse index keys.
		a_tag := binary.LittleEndian.Uint32(a[5:9])
		a_record_column := a[9:]

		b_tag := binary.LittleEndian.Uint32(b[5:9])
		b_record_column := b[9:]

		// Indexes are sorted first according to the field tag.
		if a_tag < b_tag {
			return -1
		}
		if a_tag > b_tag {
			return 1
		}

		return bytes.Compare(a_record_column, b_record_column)
	case key_type_table_column_repeatable_index:
		// Parse index keys.
		a_tag := binary.LittleEndian.Uint32(a[5:9])
		a_record_kind := storage.TableSchemaColumnKind(a[17])
		a_record_column := a[18:]

		b_tag := binary.LittleEndian.Uint32(b[5:9])
		b_record_kind := storage.TableSchemaColumnKind(b[17])
		b_record_column := b[18:]
		// Indexes are sorted first according to the field tag.
		if a_tag < b_tag {
			return -1
		}
		if a_tag > b_tag {
			return 1
		}

		if a_record_kind != b_record_kind {
			panic(fmt.Errorf("invalid column kind difference for same tag in repeatable index"))
		}

		switch a_record_kind {
		case storage.TableSchemaColumnUint:
			a_column_value := uint64(binary.LittleEndian.Uint64(a_record_column))
			b_column_value := uint64(binary.LittleEndian.Uint64(b_record_column))
			if a_column_value < b_column_value {
				return -1
			}
			if a_column_value > b_column_value {
				return 1
			}
			return 0
		case storage.TableSchemaColumnInt:
			a_column_value := int64(binary.LittleEndian.Uint64(a_record_column))
			b_column_value := int64(binary.LittleEndian.Uint64(b_record_column))
			if a_column_value < b_column_value {
				return -1
			}
			if a_column_value > b_column_value {
				return 1
			}
			return 0
		default:
			return bytes.Compare(a_record_column, b_record_column)
		}
	case key_type_table_record:
		a_record := binary.LittleEndian.Uint64(a[5:13])
		b_record := binary.LittleEndian.Uint64(b[5:13])
		if a_record < b_record {
			return -1
		}
		if a_record > b_record {
			return 1
		}
		return 0
	default:
		panic(a_key)
	}

	return 0
}

func (key_comparator *key_comparator) Name() string {
	return "leveldb_core.key_comparator"
}

func (key_comparator *key_comparator) Separator(dst, a, b []byte) []byte {
	return nil
}

func (key_comparator *key_comparator) Successor(dst, b []byte) []byte {
	return nil
}
