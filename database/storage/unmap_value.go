package storage

import (
	"fmt"
	"reflect"
	"time"
)

func unmap_array(column Record, value reflect.Value, schema *TableSchemaColumn) error {
	for i := 0; i < len(column); i++ {
		if err := unmap_column(column[i], value.Index(i), &schema.Members[0]); err != nil {
			return err
		}
	}

	return nil
}

func unmap_slice(column Record, value reflect.Value, schema *TableSchemaColumn) error {
	value.Set(reflect.MakeSlice(value.Type(), len(column), len(column)))

	for i := 0; i < len(column); i++ {
		if err := unmap_column(column[i], value.Index(i), &schema.Members[0]); err != nil {
			return err
		}
	}

	return nil
}

func unmap_map(column Record, value reflect.Value, schema *TableSchemaColumn) error {
	keys := column[0].(Record)
	values := column[1].(Record)

	key_type := value.Type().Key()
	value_type := value.Type().Elem()

	if value.IsNil() {
		value.Set(reflect.MakeMapWithSize(value.Type(), len(keys)))
	}

	for i := range keys {
		key := reflect.New(key_type).Elem()
		value := reflect.New(value_type).Elem()
		if err := unmap_column(keys[i], key, &schema.Members[0]); err != nil {
			return err
		}
		if err := unmap_column(values[i], value, &schema.Members[1]); err != nil {
			return err
		}
		value.SetMapIndex(key, value)
	}

	return nil
}

func unmap_column(column any, value reflect.Value, schema *TableSchemaColumn) error {
	switch schema.Kind {
	case TableSchemaColumnBool, TableSchemaColumnInt, TableSchemaColumnUint, TableSchemaColumnFloat, TableSchemaColumnString, TableSchemaColumnBytes:
		column_value := reflect.ValueOf(column)
		if column_value.Type() == value.Type() {
			value.Set(column_value)
		} else if column_value.CanConvert(value.Type()) {
			value.Set(column_value.Convert(value.Type()))
		} else {
			return fmt.Errorf("storage: mapped column type %s is not convertible to structured value type %s", column_value.Type(), value.Type())
		}
	case TableSchemaColumnArray:
		return unmap_array(column.(Record), value, schema)
	case TableSchemaColumnSlice:
		return unmap_slice(column.(Record), value, schema)
	case TableSchemaColumnMap:
		return unmap_map(column.(Record), value, schema)
	case TableSchemaColumnStructure:
		return unmap_structure(column.(Record), value, schema.Members)
	case TableSchemaColumnTime:
		value.Set(reflect.ValueOf(column.(time.Time)))
	default:
		return fmt.Errorf("unmap unknown kind %d", schema.Kind)
	}
	return nil
}

func unmap_structure(record Record, value reflect.Value, schema []TableSchemaColumn) error {
	for i := 0; i < len(record); i++ {
		if err := unmap_column(record[i], value.Field(i), &schema[i]); err != nil {
			return err
		}
	}

	return nil
}

// Take a Record
func UnmapReflectValue(record Record, value reflect.Value, schema *TableSchemaStructure) error {
	return unmap_structure(record, value, schema.Columns)
}
