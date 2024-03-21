package web

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"time"

	"github.com/Gophercraft/phylactery/database/query"
	"github.com/Gophercraft/phylactery/database/storage"
	"github.com/Gophercraft/phylactery/server/service/web/models"
)

var bytes_type = reflect.TypeFor[[]byte]()
var time_type = reflect.TypeFor[time.Time]()

func format_float32(x float32) string {
	str, _ := json.Marshal(x)
	return string(str)
}

func format_float64(x float64) string {
	str, _ := json.Marshal(x)
	return string(str)
}

func encode_string_json(writer io.Writer, value string) (err error) {
	_, err = writer.Write([]byte(strconv.Quote(value)))
	return
}

func encode_isomorph_json(writer io.Writer, value any) (err error) {
	rvalue := reflect.ValueOf(value)

	switch rvalue.Type() {
	case bytes_type:
		str := base64.StdEncoding.EncodeToString(value.([]byte))

		return encode_string_json(writer, str)
	case time_type:
		time_value := value.(time.Time)
		time_string := time_value.UTC().Format(time.RFC3339)
		return encode_string_json(writer, time_string)
	}

	switch rvalue.Kind() {
	case reflect.Bool:
		if rvalue.Bool() {
			return encode_string_json(writer, "1")
		} else {
			return encode_string_json(writer, "0")
		}
	case reflect.String:
		return encode_string_json(writer, rvalue.String())
	case reflect.Slice:
		if _, err = writer.Write([]byte{'['}); err != nil {
			return
		}

		ceiling := rvalue.Len() - 1

		for i := 0; i < rvalue.Len(); i++ {
			err = encode_isomorph_json(writer, rvalue.Index(i).Interface())
			if err != nil {
				return
			}

			if i != ceiling {
				if _, err = writer.Write([]byte{','}); err != nil {
					return
				}
			}
		}

		if _, err = writer.Write([]byte{']'}); err != nil {
			return
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return encode_string_json(writer, strconv.FormatUint(rvalue.Uint(), 10))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return encode_string_json(writer, strconv.FormatInt(rvalue.Int(), 10))
	case reflect.Float32:
		return encode_string_json(writer, format_float32(value.(float32)))
	case reflect.Float64:
		return encode_string_json(writer, format_float64(value.(float64)))
	default:
		return fmt.Errorf("value cannot have kind of %s", rvalue.Kind())
	}

	return
}

func encode_isomorph_records_json(records []storage.Record) (raw json.RawMessage, err error) {
	var buf bytes.Buffer
	err = encode_isomorph_json(&buf, records)
	if err != nil {
		return
	}

	raw = json.RawMessage(buf.Bytes())
	return
}

// func unmap_isomorph_json_column(column_schema *storage.TableSchemaColumn, iso_value any) (value any, err error) {
// 	rvalue := reflect.ValueOf(iso_value)

// 	switch column_schema.Kind {
// 	case storage.TableSchemaColumnUint:
// 		var uintvalue uint64
// 		uintvalue, err =
// 	case storage.TableSchemaColumnFloat:
// 	case storage.TableSchemaColumnBool:
// 	case storage.TableSchemaColumnString:
// 	case storage.TableSchemaColumnBytes:
// 	case storage.TableSchemaColumnStructure:
// 	case storage.TableSchemaColumnArray:
// 	case storage.TableSchemaColumnSlice:
// 	case storage.TableSchemaColumnMap:
// 	case storage.TableSchemaColumnTime:
// 	}
// }

// func unmap_isomorph_json_record(schema *storage.TableSchemaStructure, iso_values []any) (record storage.Record, err error) {
// 	record = make(storage.Record, len(iso_values))
// 	for i := range iso_values {
// 		if i >= len(schema.Columns) {
// 			return nil, fmt.Errorf("mismatch between schema and isomorphic value")
// 		}

// 		record[i], err = unmap_isomorph_json_column(&schema.Columns[i], iso_values[i])
// 		if err != nil {
// 			return
// 		}
// 	}

// 	return
// }

func convert_json_condition_type(json_type string) (t query.ConditionType, err error) {
	switch json_type {
	case "equals":
		t = query.Condition_Equals
	case "greater_than":
		t = query.Condition_GreaterThan
	case "less_than":
		t = query.Condition_LessThan
	case "greater_than_or_equal":
		t = query.Condition_GreaterThanOrEqual
	case "less_than_or_equal":
		t = query.Condition_LessThanOrEqual
	case "regex":
		t = query.Condition_RegularExpression
	case "not":
		t = query.Condition_Not
	case "or":
		t = query.Condition_Or
	case "band":
		t = query.Condition_BitwiseAND
	default:
		err = fmt.Errorf("unknown condition type: %s", json_type)
	}

	return
}

func convert_json_record_isomorph_value(columns []storage.TableSchemaColumn, iso_value []any) (value storage.Record, err error) {
	slice := iso_value
	result := make(storage.Record, len(slice))

	for i := range slice {
		if i >= len(columns) {
			err = fmt.Errorf("slice has more members than schema has members")
			return
		}
		column_schema := &columns[i]
		slice_value := value[i]
		var column_value any
		if column_value, err = convert_json_column_isomorph_value(column_schema, slice_value); err != nil {
			return
		}
		result[i] = column_value
	}

	return
}

// Use column schema to convert an isomorphic [](string|[]string) into a any|[]storage.Record
func convert_json_column_isomorph_value(column_schema *storage.TableSchemaColumn, iso_value any) (value any, err error) {
	rvalue := reflect.ValueOf(iso_value)

	switch column_schema.Kind {
	case storage.TableSchemaColumnUint:
		var uintvalue uint64
		uintvalue, err = strconv.ParseUint(rvalue.String(), 10, 64)
		if err != nil {
			return
		}
		switch column_schema.Size {
		case 8:
			value = uint8(uintvalue)
		case 16:
			value = uint16(uintvalue)
		case 32:
			value = uint32(uintvalue)
		case 64:
			value = uintvalue
		default:
			err = fmt.Errorf("cannot decode unknown size %d", err)
		}
	case storage.TableSchemaColumnInt:
		var intvalue int64
		intvalue, err = strconv.ParseInt(rvalue.String(), 10, int(column_schema.Size))
		if err != nil {
			return
		}
		switch column_schema.Size {
		case 8:
			value = int8(intvalue)
		case 16:
			value = int16(intvalue)
		case 32:
			value = int32(intvalue)
		case 64:
			value = intvalue
		default:
			err = fmt.Errorf("cannot decode unknown size %d", err)
		}
	case storage.TableSchemaColumnFloat:
		var floatvalue float64
		floatvalue, err = strconv.ParseFloat(rvalue.String(), int(column_schema.Size))
		if err != nil {
			return
		}
		switch column_schema.Size {
		case 32:
			value = float32(floatvalue)
		case 64:
			value = floatvalue
		default:
			err = fmt.Errorf("cannot decode unknown size %d", err)
		}
	case storage.TableSchemaColumnBool:
		if rvalue.String() == "1" {
			value = true
		} else if rvalue.String() == "0" {
			value = false
		} else {
			err = fmt.Errorf("invalid boolean %s", rvalue.String())
		}
	case storage.TableSchemaColumnString:
		value = rvalue.String()
	case storage.TableSchemaColumnBytes:
		value, err = base64.StdEncoding.DecodeString(rvalue.String())
	case storage.TableSchemaColumnStructure:
		any_slice, ok := value.([]any)
		if !ok {
			err = fmt.Errorf("structure isomorph must be array type")
			return
		}
		value, err = convert_json_record_isomorph_value(column_schema.Members, any_slice)
	case storage.TableSchemaColumnArray:
		any_slice, ok := value.([]any)
		if !ok {
			err = fmt.Errorf("array isomorph must be array type")
			return
		}
		if len(any_slice) != int(column_schema.Size) {
			err = fmt.Errorf("supplied array size does not match schema size")
			return
		}
		value, err = convert_json_record_isomorph_value(column_schema.Members, any_slice)
	case storage.TableSchemaColumnSlice:
		any_slice, ok := value.([]any)
		if !ok {
			err = fmt.Errorf("slice isomorph must be array type")
			return
		}
		value, err = convert_json_record_isomorph_value(column_schema.Members, any_slice)
	case storage.TableSchemaColumnMap:
		any_slice, ok := value.([]any)
		if !ok {
			err = fmt.Errorf("array isomorph must be array type")
			return
		}
		if len(any_slice) != 2 {
			err = fmt.Errorf("map isomorph must be associative array (2 arrays)")
			return
		}
		value, err = convert_json_record_isomorph_value(column_schema.Members, any_slice)
	case storage.TableSchemaColumnTime:
		value, err = time.Parse(time.RFC3339, rvalue.String())
		if err != nil {
			return
		}
	default:
		err = fmt.Errorf("unknown kind")
		return
	}

	return
}

// convert the JSON representation of a query expression into one that Phylactery can use
func convert_json_expression(schema *storage.TableSchemaStructure, json_expression *models.QueryExpression) (expression *query.Expression, err error) {
	expression = new(query.Expression)
	expression.Conditions = make([]query.Condition, len(json_expression.Conditions))
	for i := range expression.Conditions {
		json_condition := &json_expression.Conditions[i]
		condition := &expression.Conditions[i]
		condition.Type, err = convert_json_condition_type(json_condition.Type)
		if err != nil {
			return
		}
		condition.ColumnName = json_condition.Column

		var column *storage.TableSchemaColumn
		for c := range schema.Columns {
			column = &schema.Columns[c]
			if column.Name == condition.ColumnName {
				break
			}
		}

		if column.Name != condition.ColumnName {
			err = fmt.Errorf("column %s not found", condition.ColumnName)
			return
		}

		condition.Parameter, err = convert_json_column_isomorph_value(column, json_condition.Parameter)
		if err != nil {
			return
		}
	}
	return
}
