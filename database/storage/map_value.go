package storage

import (
	"fmt"
	"reflect"
)

// Using reflection, maps a Go struct into a storage Record.
func MapReflectValue(value reflect.Value, schema *TableSchemaStructure) (record Record, err error) {
	if value.Kind() != reflect.Struct {
		err = fmt.Errorf("map value is not a struct.")
		return
	}

	return map_structure(value, schema.Columns)
}

func map_map(map_column_value reflect.Value, map_column_schema *TableSchemaColumn) (mapped_map_column Record, err error) {
	// map[string int] =>

	// {
	//	 key array
	//   {
	//     string, string, string
	//   }
	//   value array
	//   {
	//     int, int, int
	//   }
	// }

	mapped_map_column = make(Record, 2)

	map_key_array := make(Record, map_column_value.Len())
	map_value_array := make(Record, map_column_value.Len())

	map_keys := map_column_value.MapKeys()

	for i := range map_key_array {
		map_key_array[i], err = map_column(map_keys[i], &map_column_schema.Members[0])
		if err != nil {
			return
		}
	}

	for i := range map_value_array {
		map_value_array[i], err = map_column(map_column_value.MapIndex(map_keys[i]), &map_column_schema.Members[1])
		if err != nil {
			return
		}
	}

	mapped_map_column[0] = map_key_array
	mapped_map_column[1] = map_value_array

	return
}

func map_array(array_column_value reflect.Value, array_column_schema *TableSchemaColumn) (mapped_array_column Record, err error) {
	mapped_array_column = make(Record, array_column_value.Len())
	for i := range mapped_array_column {
		mapped_array_column[i], err = map_column(array_column_value.Index(i), &array_column_schema.Members[0])
		if err != nil {
			return
		}
	}

	return
}

func map_column(column_value reflect.Value, column_schema *TableSchemaColumn) (mapped_column any, err error) {
	switch column_value.Kind() {
	case reflect.Uint, reflect.Uint64:
		return uint64(column_value.Uint()), nil
	case reflect.Uint8:
		return uint8(column_value.Uint()), nil
	case reflect.Uint16:
		return uint16(column_value.Uint()), nil
	case reflect.Uint32:
		return uint32(column_value.Uint()), nil
	case reflect.Int, reflect.Int64:
		return int64(column_value.Int()), nil
	case reflect.Int8:
		return int8(column_value.Int()), nil
	case reflect.Int16:
		return int16(column_value.Int()), nil
	case reflect.Int32:
		return int32(column_value.Int()), nil
	case reflect.Bool:
		return bool(column_value.Bool()), nil
	case reflect.Map:
		return map_map(column_value, column_schema)
	case reflect.Array, reflect.Slice:
		return map_array(column_value, column_schema)
	case reflect.String:
		return string(column_value.String()), nil
	case reflect.Struct:
		return map_structure(column_value, column_schema.Members)
	default:
		return nil, fmt.Errorf("unhandled kind %s: %s", column_schema.Name, column_value.Kind())
	}
}

func map_structure(structure reflect.Value, column_schemas []TableSchemaColumn) (mapped_record Record, err error) {
	// Make space for record columns
	mapped_record = make(Record, len(column_schemas))

	if len(mapped_record) != structure.NumField() {
		err = fmt.Errorf("mismatch between mapped record length (%d) and structure (%d)", len(mapped_record), structure.NumField())
		return
	}

	for column_index := 0; column_index < len(column_schemas); column_index++ {
		column_value := structure.Field(column_index)
		var mapped_column any
		mapped_column, err = map_column(column_value, &column_schemas[column_index])
		if err != nil {
			return
		}
		mapped_record[column_index] = mapped_column
	}

	return
}
