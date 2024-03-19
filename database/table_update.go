package database

import (
	"fmt"
	"reflect"

	"github.com/Gophercraft/phylactery/database/storage"
)

func (table_query *TableQuery) Update(record any) (affected_rows uint64, err error) {
	record_value := reflect.ValueOf(record)

	// Dereference record pointer
	if record_value.Kind() == reflect.Pointer {
		record_value = record_value.Elem()
	}

	if record_value.Kind() != reflect.Struct {
		err = fmt.Errorf("cannot update non-struct type")
		return
	}

	schema := table_query.table.Schema()
	if schema == nil {
		panic("Columns() requires schema")
	}
	var mapped_record storage.Record
	mapped_record, err = storage.MapReflectValue(record_value, schema)
	if err != nil {
		return
	}

	return table_query.UpdateRecord(mapped_record)
}

func (table_query *TableQuery) UpdateRecord(mapped_record storage.Record) (affected_rows uint64, err error) {
	mapped_columns := make([]any, len(table_query.column_indices))
	for i, index := range table_query.column_indices {
		mapped_columns[i] = mapped_record[index]
	}

	return table_query.table.container.engine.Update(
		table_query.table.table,
		&table_query.expression,
		table_query.column_indices,
		mapped_columns,
	)
}

func (table_query *TableQuery) UpdateColumns(values ...any) (affected_rows uint64, err error) {
	if len(values) != len(table_query.column_indices) {
		err = fmt.Errorf("phylactery/database: the number of named columns does not match the number of columns supplied to UpdateColumns(). Check whether you're calling Columns() with the right number of parameters")
		return
	}

	return table_query.table.container.engine.Update(
		table_query.table.table,
		&table_query.expression,
		table_query.column_indices,
		values,
	)
}
