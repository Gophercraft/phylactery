package database

import (
	"github.com/Gophercraft/phylactery/database/query"
)

type TransactionTableQuery struct {
	transaction_table *TransactionTable
	expression        query.Expression
}

// Array the results of a query by a certain column in ascending or descending fashion
func (tx_table_query *TransactionTableQuery) OrderBy(column_name string, descending bool) *TransactionTableQuery {
	schema := tx_table_query.transaction_table.Schema()
	apply_query_expression_order(schema, &tx_table_query.expression, column_name, descending)
	return tx_table_query
}

func (tx_table_query *TransactionTableQuery) Limit(limit uint64) *TransactionTableQuery {
	tx_table_query.expression.Limit = limit
	return tx_table_query
}

// Find a single record matched by the query
func (tx_table_query *TransactionTableQuery) Get(single any) (found bool, err error) {
	tx_table_query.Limit(1)
	table_id := tx_table_query.transaction_table.table_id
	schema := tx_table_query.transaction_table.Schema()
	return get_record(
		table_id,
		schema,
		tx_table_query.transaction_table.transaction.storage_transaction,
		&tx_table_query.expression,
		single,
	)
}

// Collect multiple records into an array passed by reference
func (tx_table_query *TransactionTableQuery) Find(multiple any) (err error) {
	schema := tx_table_query.transaction_table.Schema()
	return find_records(
		tx_table_query.transaction_table.table_id,
		schema,
		tx_table_query.transaction_table.transaction.storage_transaction,
		&tx_table_query.expression,
		multiple,
	)
}

func (tx_table_query *TransactionTableQuery) Count() (records uint64, err error) {
	return tx_table_query.transaction_table.transaction.storage_transaction.Count(
		tx_table_query.transaction_table.table_id,
		&tx_table_query.expression)
}

func (tx_table_query *TransactionTableQuery) Delete() (deleted uint64, err error) {
	return tx_table_query.transaction_table.transaction.storage_transaction.Delete(
		tx_table_query.transaction_table.table_id,
		&tx_table_query.expression,
	)
}
