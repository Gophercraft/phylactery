package database

import (
	"fmt"
	"reflect"

	"github.com/Gophercraft/phylactery/database/query"
	"github.com/Gophercraft/phylactery/database/storage"
)

func prepare_query_condition(condition *query.Condition, schema *storage.TableSchemaStructure) {
	condition.Column = -1
	for column_index, column := range schema.Columns {
		if column.Name == condition.ColumnName {
			condition.Column = column_index
			break
		}

		if condition.Type == query.Condition_Not {
			prepare_query_condition(condition.Parameter.(*query.Condition), schema)
			return
		}
	}
	// Was ColumnName found in the schema?
	if condition.Column == -1 {
		panic(fmt.Errorf("invalid query.Condition: no column name '%s", condition.ColumnName))
	}
}

func prepare_query_expression(expression *query.Expression, schema *storage.TableSchemaStructure) {
	for i := range expression.Conditions {
		// Look for Column with the supplied name.
		prepare_query_condition(&expression.Conditions[i], schema)
	}
}

func apply_query_expression_order(schema *storage.TableSchemaStructure, expr *query.Expression, column_name string, descending bool) {
	for index, column := range schema.Columns {
		if column.Name == column_name {
			expr.Sort = true
			expr.OrderByColumnIndex = index
			expr.Descending = descending
			return
		}
	}

	panic(column_name)
}

func apply_column_indices(schema *storage.TableSchemaStructure, expr *query.Expression, column_names []string) (column_indices []int) {
	found := make([]bool, len(column_names))
	column_indices = make([]int, len(column_names))

	for indices_index, column_name := range column_names {
		for column_index, column := range schema.Columns {
			if column_name == column.Name {
				column_indices[indices_index] = column_index
				found[indices_index] = true
				break
			}
		}
	}

	for index, fn := range found {
		if !fn {
			panic(fmt.Errorf("could not find column %s in schema", column_names[index]))
		}
	}

	return
}

type table_query interface {
	Query(table int32, expr *query.Expression) ([]storage.Record, error)
}

func get_record(table_id int32, schema *storage.TableSchemaStructure, table table_query, expr *query.Expression, single any) (found bool, err error) {
	if schema == nil {
		err = fmt.Errorf("[table %d] cannot Get() without a table schema", table_id)
		return
	}

	var rows []storage.Record
	rows, err = table.Query(table_id, expr)
	if err != nil {
		return
	}

	single_value := reflect.ValueOf(single)
	if single_value.Kind() == reflect.Pointer {
		single_value = single_value.Elem()
	}

	if len(rows) > 0 {
		// Found
		return true, storage.UnmapReflectValue(rows[0], single_value, schema)
	}

	// Did not find, but this is not an error
	return false, nil
}

func find_records(table_id int32, schema *storage.TableSchemaStructure, table table_query, expr *query.Expression, multiple any) (err error) {
	if schema == nil {
		err = fmt.Errorf("[table %d] cannot Find() without a table schema", table_id)
		return
	}
	// Perform query and get mapped data
	var rows []storage.Record
	rows, err = table.Query(table_id, expr)
	if err != nil {
		return
	}

	if len(rows) == 0 {
		err = nil
		return
	}

	// Begin reflecting on result pointer
	slice_value := reflect.ValueOf(multiple)
	if slice_value.Kind() != reflect.Pointer {
		err = fmt.Errorf("cannot Find to non-pointer-slice")
		return
	}
	// Dereference pointer to slice
	slice_value = slice_value.Elem()
	if slice_value.Kind() != reflect.Slice {
		err = fmt.Errorf("cannot Find to non-slice")
		return
	}

	// Allocate space for results
	slice_value.Set(reflect.MakeSlice(slice_value.Type(), len(rows), len(rows)))

	// Convert records to structs
	for i := range rows {
		if err = storage.UnmapReflectValue(rows[i], slice_value.Index(i), schema); err != nil {
			return err
		}
	}

	return
}
