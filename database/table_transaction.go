package database

import (
	"fmt"
	"reflect"

	"github.com/Gophercraft/phylactery/database/query"
	"github.com/Gophercraft/phylactery/database/storage"
)

// Same as Table, just for a transaction instead of a raw query
type TransactionTable struct {
	transaction *Transaction
	table_id    int32
}

func (tx_table *TransactionTable) Insert(records any) (err error) {
	records_value := reflect.ValueOf(records)
	if records_value.Kind() == reflect.Pointer {
		records_value = records_value.Elem()
	}

	if records_value.Kind() == reflect.Slice {
		schema := tx_table.transaction.container.engine.Schema(tx_table.table_id)
		mapped_records := make([]storage.Record, records_value.Len())
		for i := 0; i < records_value.Len(); i++ {
			mapped_records[i], err = storage.MapReflectValue(records_value.Index(i), schema)
			if err != nil {
				return
			}
		}
		return tx_table.InsertRecords(mapped_records)
	}

	if records_value.Kind() == reflect.Struct {
		schema := tx_table.transaction.container.engine.Schema(tx_table.table_id)
		mapped_records := make([]storage.Record, 1)
		mapped_records[0], err = storage.MapReflectValue(records_value, schema)
		if err != nil {
			return err
		}
		return tx_table.InsertRecords(mapped_records)
	}

	return fmt.Errorf("invalid type")
}

func (tx_table *TransactionTable) InsertRecords(records []storage.Record) error {
	return tx_table.transaction.storage_transaction.Insert(tx_table.table_id, records)
}

func (tx_table *TransactionTable) Schema() *storage.TableSchemaStructure {
	return tx_table.transaction.container.engine.Schema(tx_table.table_id)
}

type TransactionTableQuery struct {
	transaction_table *TransactionTable
	expression        query.Expression
}

func (tx_table *TransactionTable) Where(conditions ...query.Condition) *TransactionTableQuery {
	transaction_table_query := new(TransactionTableQuery)
	transaction_table_query.transaction_table = tx_table
	transaction_table_query.expression.Conditions = conditions

	schema := tx_table.Schema()
	prepare_query_expression(&transaction_table_query.expression, schema)
	return transaction_table_query
}
