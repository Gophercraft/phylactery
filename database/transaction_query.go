package database

import (
	"fmt"
	"reflect"

	"github.com/Gophercraft/phylactery/database/query"
	"github.com/Gophercraft/phylactery/database/storage"
)

type TransactionTableQuery struct {
	transaction_table *TransactionTable
	expression        query.Expression
	column_indices    []int
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

func (tx_table_query *TransactionTableQuery) Columns(names ...string) *TransactionTableQuery {
	schema := tx_table_query.transaction_table.Schema()
	if schema == nil {
		panic("Columns() requires schema")
	}
	tx_table_query.column_indices = apply_column_indices(schema, &tx_table_query.expression, names)
	return tx_table_query
}

func (tx_table_query *TransactionTableQuery) Update(record any) (affected_rows uint64, err error) {
	record_value := reflect.ValueOf(record)

	// Dereference record pointer
	if record_value.Kind() == reflect.Pointer {
		record_value = record_value.Elem()
	}

	if record_value.Kind() != reflect.Struct {
		err = fmt.Errorf("cannot update non-struct type")
		return
	}

	schema := tx_table_query.transaction_table.Schema()
	if schema == nil {
		panic("Columns() requires schema")
	}
	var mapped_record storage.Record
	mapped_record, err = storage.MapReflectValue(record_value, schema)
	if err != nil {
		return
	}
	return tx_table_query.UpdateRecord(mapped_record)
}

func (tx_table_query *TransactionTableQuery) UpdateRecord(mapped_record storage.Record) (affected_rows uint64, err error) {
	mapped_columns := make([]any, len(tx_table_query.column_indices))
	for i, index := range tx_table_query.column_indices {
		mapped_columns[i] = mapped_record[index]
	}

	return tx_table_query.transaction_table.transaction.container.engine.Update(
		tx_table_query.transaction_table.table_id,
		&tx_table_query.expression,
		tx_table_query.column_indices,
		mapped_columns,
	)
}
