package database

import (
	"fmt"
	"reflect"

	"github.com/Gophercraft/phylactery/database/query"
	"github.com/Gophercraft/phylactery/database/storage"
)

type TableQuery struct {
	table      *Table
	expression query.Expression
}

// Create a table query where the result is informed by the supplied conditions
// The conditions are modified to supply the faster index-reference of columns instead of relying on (*query.Condition).ColumnName
func (table *Table) Where(conditions ...query.Condition) *TableQuery {
	table_query := new(TableQuery)
	table_query.table = table
	table_query.expression.Conditions = conditions

	schema := table.Schema()
	if schema == nil {
		panic(fmt.Errorf("database: [table %d] cannot use Where() clause without first defining a schema with Sync()", table.table))
	}

	prepare_query_expression(&table_query.expression, schema)
	return table_query
}

func (table_query *TableQuery) Limit(limit uint64) *TableQuery {
	table_query.expression.Limit = limit
	return table_query
}

// Array the results of a query by a certain column in ascending or descending fashion
func (table_query *TableQuery) OrderBy(column_name string, descending bool) *TableQuery {
	schema := table_query.table.Schema()
	for index, column := range schema.Columns {
		if column.Name == column_name {
			table_query.expression.Sort = true
			table_query.expression.OrderByColumnIndex = index
			table_query.expression.Descending = descending
			return table_query
		}
	}

	panic(column_name)
}

// Look up a single record that satifies the table query
func (table_query *TableQuery) Get(single any) (found bool, err error) {
	table_query.Limit(1)
	var rows []storage.Record
	rows, err = table_query.table.container.engine.Query(table_query.table.table, &table_query.expression)
	if err != nil {
		return
	}

	single_value := reflect.ValueOf(single)
	if single_value.Kind() == reflect.Pointer {
		single_value = single_value.Elem()
	}

	if len(rows) > 0 {
		// Found
		return true, storage.UnmapReflectValue(rows[0], single_value, table_query.table.Schema())
	}

	// Did not find, but this is not an error
	return false, nil
}

// Collect multiple records into an array passed by reference
func (table_query *TableQuery) Find(multiple any) (err error) {
	var rows []storage.Record
	rows, err = table_query.table.container.engine.Query(table_query.table.table, &table_query.expression)
	if err != nil {
		return
	}

	if len(rows) == 0 {
		err = nil
		return
	}

	schema := table_query.table.Schema()

	slice_value := reflect.ValueOf(multiple)
	if slice_value.Kind() != reflect.Pointer {
		err = fmt.Errorf("cannot Find to non-pointer-slice")
		return
	}
	slice_value = slice_value.Elem()
	if slice_value.Kind() != reflect.Slice {
		err = fmt.Errorf("cannot Find to non-slice")
	}

	slice_value.Set(reflect.MakeSlice(slice_value.Type(), len(rows), len(rows)))

	for i := range rows {
		if err = storage.UnmapReflectValue(rows[i], slice_value.Index(i), schema); err != nil {
			return err
		}
	}

	return
}

func (table_query *TableQuery) Delete() (deleted uint64, err error) {
	return table_query.table.container.engine.Delete(table_query.table.table, &table_query.expression)
}
