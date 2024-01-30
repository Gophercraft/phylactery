package database

import (
	"github.com/Gophercraft/phylactery/database/storage"
)

// Insert records into the database after having converted your data to the proper format.
// This is the reflection-free alternative to (*Table).Insert().
func (table *Table) InsertRecords(records []storage.Record) error {
	return table.container.engine.Insert(table.table, records)
}

// Insert structs into the table using Go reflection.
func (table *Table) Insert(records any) (err error) {
	schema := table.container.engine.Schema(table.table)
	err = insert_records(table, schema, records)
	return
}
