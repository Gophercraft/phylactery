package storage

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

var (
	spec_time = reflect.TypeOf(new(time.Time)).Elem()
)

var acceptable_index_types = []TableSchemaColumnKind{
	TableSchemaColumnUint,
	TableSchemaColumnInt,
	TableSchemaColumnFloat,
	TableSchemaColumnString,
}

func schematize_column(column_type reflect.Type) (column TableSchemaColumn, err error) {
	// schematize special types
	switch column_type {
	case spec_time:
		column.Kind = TableSchemaColumnTime
		return
	}
	// schematize based on the kind of the type
	switch column_type.Kind() {
	case reflect.String:
		column.Kind = TableSchemaColumnString
	case reflect.Bool:
		// boolean
		column.Kind = TableSchemaColumnBool
	case reflect.Uint:
		// Unsigned integer
		column.Kind = TableSchemaColumnUint
		column.Size = 64
	case reflect.Uint8:
		column.Kind = TableSchemaColumnUint
		column.Size = 8
	case reflect.Uint16:
		column.Kind = TableSchemaColumnUint
		column.Size = 16
	case reflect.Uint32:
		column.Kind = TableSchemaColumnUint
		column.Size = 32
	case reflect.Uint64:
		column.Kind = TableSchemaColumnUint
		column.Size = 64
		// Signed integer
	case reflect.Int:
		column.Kind = TableSchemaColumnInt
		column.Size = 64
	case reflect.Int8:
		column.Kind = TableSchemaColumnInt
		column.Size = 8
	case reflect.Int16:
		column.Kind = TableSchemaColumnInt
		column.Size = 16
	case reflect.Int32:
		column.Kind = TableSchemaColumnInt
		column.Size = 32
	case reflect.Int64:
		column.Kind = TableSchemaColumnInt
		column.Size = 64
		// floating point
	case reflect.Float32:
		column.Kind = TableSchemaColumnFloat
		column.Size = 32
	case reflect.Float64:
		column.Kind = TableSchemaColumnFloat
		column.Size = 64
	case reflect.Array:
		column.Kind = TableSchemaColumnArray
		// Get fixed size of array
		column.Size = int32(column_type.Len())
		// Place single type schema of array content inside Members slice
		var element TableSchemaColumn
		element, err = schematize_column(column_type.Elem())
		if err != nil {
			return
		}
		column.Members = []TableSchemaColumn{element}
	case reflect.Slice:
		// Variable size array or slice
		column.Kind = TableSchemaColumnSlice
		var element TableSchemaColumn
		// Place single type schema of slice content inside Members slice
		element, err = schematize_column(column_type.Elem())
		if err != nil {
			return
		}
		column.Members = []TableSchemaColumn{element}
	case reflect.Struct:
		column.Kind = TableSchemaColumnStructure
		column.Members, err = schematize_structure(column_type)
		if err != nil {
			return
		}
	case reflect.Map:
		column.Members = make([]TableSchemaColumn, 2)
		column.Kind = TableSchemaColumnMap
		column.Members[0], err = schematize_column(column_type.Key())
		if err != nil {
			return
		}
		column.Members[1], err = schematize_column(column_type.Elem())
		if err != nil {
			return
		}
	default:
		err = fmt.Errorf("don't know how to encode this kind %s", column_type.Kind())
	}

	return
}

func schematize_structure(structure_type reflect.Type) (columns []TableSchemaColumn, err error) {
	if structure_type.Kind() != reflect.Struct {
		return nil, fmt.Errorf("not a structure type")
	}

	last_tag := uint32(0)

	for i := 0; i < structure_type.NumField(); i++ {
		// Schematize field type
		field := structure_type.Field(i)
		var column_schema TableSchemaColumn
		column_schema, err = schematize_column(field.Type)
		if err != nil {
			return
		}

		// Get field name into schema
		column_schema.Name = field.Name

		// Schematize tag information
		field_tag_string := field.Tag.Get("database")
		var field_tag_number uint32 = last_tag + 1
		var field_tag_options_string string
		if strings.Contains(field_tag_string, ":") {
			field_tag_elements := strings.SplitN(field_tag_string, ":", 2)
			var field_tag_number_u64 uint64
			field_tag_number_u64, err = strconv.ParseUint(field_tag_elements[0], 10, 64)
			if err != nil {
				err = fmt.Errorf("field must have tag number %w", err)
				return
			}
			field_tag_number = uint32(field_tag_number_u64)
			field_tag_options_string = field_tag_elements[1]
		} else {
			if len(field_tag_string) > 0 {
				var field_tag_number_u64 uint64
				field_tag_number_u64, err = strconv.ParseUint(field_tag_string, 10, 64)
				if err != nil {
					err = fmt.Errorf("field (type %s) must have tag number (%s) %w", structure_type, field_tag_string, err)
					return
				}
				field_tag_number = uint32(field_tag_number_u64)
			}
		}
		last_tag = field_tag_number
		column_schema.Tag = uint32(field_tag_number)
		if field_tag_options_string != "" {
			field_option_tags := strings.Split(field_tag_options_string, ",")
			for _, tag := range field_option_tags {
				switch tag {
				case "index":
					column_schema.Index = true
				case "exclusive":
					column_schema.Exclusive = true
				case "auto_increment":
					column_schema.AutoIncrement = true
				default:
					err = fmt.Errorf("unrecognized database tag %s (%s)", tag, field_tag_options_string)
				}
				if err != nil {
					return
				}
			}
		}

		if column_schema.Exclusive && !column_schema.Index {
			err = fmt.Errorf("field %s cannot be exclusive but not have an index", field.Name)
			return
		}

		if column_schema.Index {
			is_valid := false
			for _, accepted := range acceptable_index_types {
				if accepted == column_schema.Kind {
					is_valid = true
					break
				}
			}

			if !is_valid {
				err = fmt.Errorf("column %s with kind %d is not indexable", column_schema.Name, column_schema.Kind)
				return
			}
		}

		if err != nil {
			return
		}
		columns = append(columns, column_schema)
	}

	return
}

func SchematizeStructureType(structure_type reflect.Type) (*TableSchemaStructure, error) {
	schema := new(TableSchemaStructure)
	var err error
	schema.Columns, err = schematize_structure(structure_type)
	return schema, err
}
