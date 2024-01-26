package database

import (
	"reflect"

	"github.com/Gophercraft/phylactery/database/storage"
	"github.com/davecgh/go-spew/spew"
)

// Insert records into the database after having converted your data to the proper format.
// This is the reflection-free alternative to (*Table).Insert().
func (table *Table) InsertRecords(records []storage.Record) error {
	return table.container.engine.Insert(table.table, records)
}

// Insert structs into the table using Go reflection.
func (table *Table) Insert(records any) (err error) {
	records_value := reflect.ValueOf(records)

	if records_value.Kind() == reflect.Pointer {
		records_value = records_value.Elem()
	}

	schema := table.container.engine.Schema(table.table)
	if records_value.Kind() == reflect.Struct {
		var mapped_record storage.Record
		mapped_record, err = storage.MapReflectValue(records_value, schema)
		if err != nil {
			return
		}
		var mapped_records = make([]storage.Record, 1)
		mapped_records[0] = mapped_record
		spew.Sdump(mapped_record)
		if err = table.InsertRecords(mapped_records); err != nil {
			return err
		}
		if err = storage.UnmapReflectValue(mapped_records[0], records_value, schema); err != nil {
			panic(err)
		}
		return
	}

	mapped_records := make([]storage.Record, records_value.Len())
	for i := 0; i < records_value.Len(); i++ {
		// Map Go struct to a
		mapped_records[i], err = storage.MapReflectValue(records_value.Index(i), schema)
		if err != nil {
			return
		}
	}
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
