package database

import (
	"reflect"

	"github.com/Gophercraft/phylactery/database/storage"
)

// interface for inserting mapped records
// (either a directly accessed table or a transacted table)
type table_inserter interface {
	InsertRecords(records []storage.Record) error
}

func insert_records(table table_inserter, schema *storage.TableSchemaStructure, records any) (err error) {
	// Begin reflecting on inserted record
	records_value := reflect.ValueOf(records)

	// Dereference record pointer
	if records_value.Kind() == reflect.Pointer {
		records_value = records_value.Elem()
	}

	// If records_value points to a single struct
	if records_value.Kind() == reflect.Struct {
		var mapped_record storage.Record
		mapped_record, err = storage.MapReflectValue(records_value, schema)
		if err != nil {
			return
		}
		// Insert struct as a 1-long slice
		var mapped_records = make([]storage.Record, 1)
		mapped_records[0] = mapped_record
		if err = table.InsertRecords(mapped_records); err != nil {
			return err
		}
		// Unmap records back to struct (so that auto-increments, etc can be read back)
		if err = storage.UnmapReflectValue(mapped_records[0], records_value, schema); err != nil {
			panic(err)
		}
		return
	}

	// If records_value points to an array of structs, map them into storage.Records
	mapped_records := make([]storage.Record, records_value.Len())
	for i := 0; i < records_value.Len(); i++ {
		// Map Go struct to a
		mapped_records[i], err = storage.MapReflectValue(records_value.Index(i), schema)
		if err != nil {
			return
		}
	}
	// Insert records into storage engine/transaction
	if err := table.InsertRecords(mapped_records); err != nil {
		return err
	}
	// Unmap Go struct back to to array
	for index, record := range mapped_records {
		if err := storage.UnmapReflectValue(record, records_value.Index(index), schema); err != nil {
			panic(err)
		}
	}
	return
}
