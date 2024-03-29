package leveldb_core

import (
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"time"

	"github.com/Gophercraft/phylactery/database/query"
	"github.com/Gophercraft/phylactery/database/storage"
	"github.com/Gophercraft/phylactery/database/storage/record"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var time_type = reflect.TypeFor[time.Time]()

func query_get_column_and_condition(col any, condition *query.Condition) (reflect_col reflect.Value, reflect_condition_value reflect.Value, err error) {
	reflect_col = reflect.ValueOf(col)
	reflect_condition_value = reflect.ValueOf(condition.Parameter)
	if reflect_col.Kind() != reflect_condition_value.Kind() {
		err = fmt.Errorf("cannot compare different kinds of fields (%s in column(%s), queried(%s))", condition.ColumnName, reflect_col.Type(), reflect_condition_value.Type())
	}
	return
}

func query_column_matches_condition_equals(col any, schema *storage.TableSchemaColumn, condition *query.Condition) (bool, error) {
	// Get reflection values of queried value & the value of the current row
	reflect_col, reflect_condition_value, err := query_get_column_and_condition(col, condition)
	if err != nil {
		return false, err
	}

	if reflect_col.Type() == time_type {
		reflect_col_time := reflect_col.Interface().(time.Time)
		condition_time := reflect_condition_value.Interface().(time.Time)
		return reflect_col_time.Equal(condition_time), nil
	}

	switch reflect_col.Kind() {
	case reflect.String:
		return reflect_col.String() == reflect_condition_value.String(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return reflect_col.Uint() == reflect_condition_value.Uint(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return reflect_col.Int() == reflect_condition_value.Int(), nil
	case reflect.Float32:
		return reflect_col.Interface().(float32) == reflect_condition_value.Interface().(float32), nil
	case reflect.Float64:
		return reflect_col.Interface().(float64) == reflect_condition_value.Interface().(float64), nil
	case reflect.Bool:
		return reflect_col.Bool() == reflect_condition_value.Bool(), nil
	default:
		panic(fmt.Errorf("cannot == compare this kind of data %s", reflect_col))
	}
}

func query_column_matches_condition_greater_than(col any, schema *storage.TableSchemaColumn, condition *query.Condition) (bool, error) {
	// Get reflection values of queried value & the value of the current row
	reflect_col, reflect_condition_value, err := query_get_column_and_condition(col, condition)
	if err != nil {
		return false, err
	}

	if reflect_col.Type() == time_type {
		reflect_col_time := reflect_col.Interface().(time.Time)
		condition_time := reflect_condition_value.Interface().(time.Time)
		return reflect_col_time.After(condition_time), nil
	}

	switch reflect_col.Kind() {
	case reflect.String:
		return reflect_col.String() > reflect_condition_value.String(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return reflect_col.Uint() > reflect_condition_value.Uint(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return reflect_col.Int() > reflect_condition_value.Int(), nil
	case reflect.Float32:
		return reflect_col.Interface().(float32) > reflect_condition_value.Interface().(float32), nil
	case reflect.Float64:
		return reflect_col.Interface().(float64) > reflect_condition_value.Interface().(float64), nil
	default:
		panic(fmt.Errorf("cannot compare this kind of data %s", reflect_col))
	}
}

func query_column_matches_condition_less_than(col any, schema *storage.TableSchemaColumn, condition *query.Condition) (bool, error) {
	// Get reflection values of queried value & the value of the current row
	reflect_col, reflect_condition_value, err := query_get_column_and_condition(col, condition)
	if err != nil {
		return false, err
	}

	if reflect_col.Type() == time_type {
		reflect_col_time := reflect_col.Interface().(time.Time)
		condition_time := reflect_condition_value.Interface().(time.Time)
		return reflect_col_time.Before(condition_time), nil
	}

	switch reflect_col.Kind() {
	case reflect.String:
		return reflect_col.String() < reflect_condition_value.String(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return reflect_col.Uint() < reflect_condition_value.Uint(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return reflect_col.Int() < reflect_condition_value.Int(), nil
	case reflect.Float32:
		return reflect_col.Interface().(float32) < reflect_condition_value.Interface().(float32), nil
	case reflect.Float64:
		return reflect_col.Interface().(float64) < reflect_condition_value.Interface().(float64), nil
	default:
		panic(fmt.Errorf("cannot compare this kind of data %s", reflect_col))
	}
}

func query_column_matches_condition_greater_than_or_equal(col any, schema *storage.TableSchemaColumn, condition *query.Condition) (bool, error) {
	// Get reflection values of queried value & the value of the current row
	reflect_col, reflect_condition_value, err := query_get_column_and_condition(col, condition)
	if err != nil {
		return false, err
	}

	if reflect_col.Type() == time_type {
		reflect_col_time := reflect_col.Interface().(time.Time)
		condition_time := reflect_condition_value.Interface().(time.Time)
		return reflect_col_time.After(condition_time) || reflect_col_time.Equal(condition_time), nil
	}

	switch reflect_col.Kind() {
	case reflect.String:
		return reflect_col.String() >= reflect_condition_value.String(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return reflect_col.Uint() >= reflect_condition_value.Uint(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return reflect_col.Int() >= reflect_condition_value.Int(), nil
	case reflect.Float32:
		return reflect_col.Interface().(float32) >= reflect_condition_value.Interface().(float32), nil
	case reflect.Float64:
		return reflect_col.Interface().(float64) >= reflect_condition_value.Interface().(float64), nil
	default:
		panic(fmt.Errorf("cannot compare this kind of data %s", reflect_col))
	}
}

func query_column_matches_condition_less_than_or_equal(col any, schema *storage.TableSchemaColumn, condition *query.Condition) (bool, error) {
	// Get reflection values of queried value & the value of the current row
	reflect_col, reflect_condition_value, err := query_get_column_and_condition(col, condition)
	if err != nil {
		return false, err
	}

	if reflect_col.Type() == time_type {
		reflect_col_time := reflect_col.Interface().(time.Time)
		condition_time := reflect_condition_value.Interface().(time.Time)
		return reflect_col_time.Before(condition_time) || reflect_col_time.Equal(condition_time), nil
	}

	switch reflect_col.Kind() {
	case reflect.String:
		return reflect_col.String() <= reflect_condition_value.String(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return reflect_col.Uint() <= reflect_condition_value.Uint(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return reflect_col.Int() <= reflect_condition_value.Int(), nil
	case reflect.Float32:
		return reflect_col.Interface().(float32) <= reflect_condition_value.Interface().(float32), nil
	case reflect.Float64:
		return reflect_col.Interface().(float64) <= reflect_condition_value.Interface().(float64), nil
	default:
		panic(fmt.Errorf("cannot compare this kind of data %s", reflect_col))
	}
}

func query_column_matches_condition_regular_expression(col any, schema *storage.TableSchemaColumn, condition *query.Condition) (bool, error) {
	str, ok := col.(string)
	if !ok {
		return false, fmt.Errorf("field %s is not a string, cannot perform regex query on it", schema.Name)
	}

	regex := condition.Parameter.(*regexp.Regexp)

	return regex.MatchString(str), nil
}

func query_column_matches_condition_bitwise_and(col any, schema *storage.TableSchemaColumn, condition *query.Condition) (bool, error) {
	// Get reflection values of queried value & the value of the current row
	reflect_col, reflect_condition_value, err := query_get_column_and_condition(col, condition)
	if err != nil {
		return false, err
	}

	switch reflect_col.Kind() {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return (reflect_col.Uint() & reflect_condition_value.Uint()) != 0, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return (reflect_col.Int() & reflect_condition_value.Int()) != 0, nil
	default:
		panic(fmt.Errorf("cannot compare this kind of data %s", reflect_col))
	}
}

func query_match_condition(schema *storage.TableSchemaStructure, value_record storage.Record, condition *query.Condition) (bool, error) {
	switch condition.Type {
	case query.Condition_Not,
		query.Condition_Or:
		return query_match_embedded_condition(schema, value_record, condition)
	default:
		column := value_record[condition.Column]

		matched, err := query_column_matches_condition(column, &schema.Columns[condition.Column], condition)
		if err != nil {
			return false, err
		}

		return matched, nil
	}
}

func query_match_embedded_condition(schema *storage.TableSchemaStructure, value_record storage.Record, condition *query.Condition) (bool, error) {
	switch condition.Type {
	case query.Condition_Not:
		embedded_condition := condition.Parameter.(*query.Condition)
		result, err := query_match_condition(schema, value_record, embedded_condition)
		return !result, err
	case query.Condition_Or:
		embedded_conditions := condition.Parameter.([]query.Condition)
		for i := range embedded_conditions {
			embedded_condition := &embedded_conditions[i]
			matched, err := query_match_condition(schema, value_record, embedded_condition)
			if err != nil {
				return false, err
			}
			if matched {
				return true, nil
			}
		}
		return false, nil
	default:
		return false, fmt.Errorf("not an embedded condition")
	}
}

func query_column_matches_condition(col any, schema *storage.TableSchemaColumn, condition *query.Condition) (bool, error) {
	switch condition.Type {
	case query.Condition_Equals:
		return query_column_matches_condition_equals(col, schema, condition)
	case query.Condition_GreaterThan:
		return query_column_matches_condition_greater_than(col, schema, condition)
	case query.Condition_LessThan:
		return query_column_matches_condition_less_than(col, schema, condition)
	case query.Condition_GreaterThanOrEqual:
		return query_column_matches_condition_greater_than_or_equal(col, schema, condition)
	case query.Condition_LessThanOrEqual:
		return query_column_matches_condition_less_than_or_equal(col, schema, condition)
	case query.Condition_RegularExpression:
		return query_column_matches_condition_regular_expression(col, schema, condition)
	case query.Condition_Not:
		base_condition := condition.Parameter.(*query.Condition)
		matched, err := query_column_matches_condition(col, schema, base_condition)
		if err != nil {
			return false, err
		}
		return !matched, nil
	case query.Condition_BitwiseAND:
		return query_column_matches_condition_bitwise_and(col, schema, condition)
	default:
		panic(condition.Type)
	}
}

type snapshot interface {
	Get(b []byte, readopt *opt.ReadOptions) ([]byte, error)
	NewIterator(ur *util.Range, readopt *opt.ReadOptions) iterator.Iterator
}

func (engine *engine) query_match_all_records(table_id int32, snap snapshot, schema *storage.TableSchemaStructure, query_expression *query.Expression) (records []storage.Record, ids []uint64, err error) {
	var range_iteration util.Range
	var read_options opt.ReadOptions
	range_iteration.Start = make_record_sector_key(table_id, 0)
	range_iteration.Limit = make_record_sector_key(table_id, math.MaxUint64)

	iter := snap.NewIterator(&range_iteration, &read_options)

	records, ids, err = query_match_iterator_all_records(table_id, iter, schema, query_expression)
	return
}

// (SLOW!!!) iterate through all records
func query_match_iterator_all_records(table_id int32, iter iterator.Iterator, schema *storage.TableSchemaStructure, query_expression *query.Expression) (records []storage.Record, record_IDs []uint64, err error) {
	limited := query_expression.Limit > 0

	for iter.Next() {
		// Stop iteration once limit is reached.
		if limited && len(records) >= int(query_expression.Limit) {
			break
		}

		// Get key & value from LevelDB instance
		key := iter.Key()
		value := iter.Value()

		// Sanity check
		// TODO disable once we are certain that the iterator is ordered correctly
		key_table_id := int32(binary.LittleEndian.Uint32(key[0:4]))
		key_type := key_type(key[4])
		if !(key_type == key_type_table_record && key_table_id == table_id) {
			panic(fmt.Errorf("invalid record in iterator, there must be a key sorting failure (key type %d, table id %d)", key_type, key_table_id))
		}
		key_record_ID := binary.LittleEndian.Uint64(key[5:13])

		// Unmarshal value from LevelDB into a Record
		var value_record storage.Record
		value_record, err = record.Unmarshal(schema, value)
		if err != nil {
			return
		}

		// Matched = should we add this to the list?
		var matched bool

		// Range through all of the query conditions, rejecting the record if a condition isn't met
		for c := range query_expression.Conditions {
			condition := &query_expression.Conditions[c]

			matched, err = query_match_condition(schema, value_record, condition)
			if err != nil {
				return
			}

			if !matched {
				break
			}
		}

		if matched {
			records = append(records, value_record)
			record_IDs = append(record_IDs, key_record_ID)
		}
	}

	iter.Release()
	return
}
