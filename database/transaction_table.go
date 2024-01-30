package database

import (
	"github.com/Gophercraft/phylactery/database/query"
	"github.com/Gophercraft/phylactery/database/storage"
)

func (tx *Transaction) Table(table_name string) *TransactionTable {
	table_id := tx.container.engine.CreateTable(table_name)
	table := new(TransactionTable)
	table.table_id = table_id
	table.transaction = tx
	return table
}

// Same as Table, just for a transaction instead of a raw query
type TransactionTable struct {
	transaction *Transaction
	table_id    int32
}

func (tx_table *TransactionTable) InsertRecords(records []storage.Record) error {
	return tx_table.transaction.storage_transaction.Insert(tx_table.table_id, records)
}

func (tx_table *TransactionTable) Schema() *storage.TableSchemaStructure {
	return tx_table.transaction.container.engine.Schema(tx_table.table_id)
}

func (tx_table *TransactionTable) Where(conditions ...query.Condition) *TransactionTableQuery {
	transaction_table_query := new(TransactionTableQuery)
	transaction_table_query.transaction_table = tx_table
	transaction_table_query.expression.Conditions = conditions

	schema := tx_table.Schema()
	prepare_query_expression(&transaction_table_query.expression, schema)
	return transaction_table_query
}

func (tx_table *TransactionTable) Insert(records any) error {
	schema := tx_table.Schema()
	return insert_records(tx_table, schema, records)
}
