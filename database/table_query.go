package database

import (
	"fmt"

	"github.com/Gophercraft/phylactery/database/query"
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
	apply_query_expression_order(schema, &table_query.expression, column_name, descending)
	return table_query
}

// Look up a single record that matches this query
func (table_query *TableQuery) Get(single any) (found bool, err error) {
	table_query.Limit(1)
	table_id := table_query.table.table
	schema := table_query.table.Schema()
	return get_record(
		table_id,
		schema,
		table_query.table.container.engine,
		&table_query.expression,
		single,
	)
}

// Collect multiple records into an array passed by reference
func (table_query *TableQuery) Find(multiple any) (err error) {
	table_id := table_query.table.table
	schema := table_query.table.Schema()
	return find_records(table_id, schema, table_query.table.container.engine, &table_query.expression, multiple)
}

func (table_query *TableQuery) Delete() (deleted uint64, err error) {
	return table_query.table.container.engine.Delete(table_query.table.table, &table_query.expression)
}

func (table_query *TableQuery) Count() (records uint64, err error) {
	return table_query.table.container.engine.Count(
		table_query.table.table,
		&table_query.expression)
}
