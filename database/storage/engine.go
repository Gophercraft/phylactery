package storage

import "github.com/Gophercraft/phylactery/database/query"

type Iteration func(record Record) (continue_iterating bool)

type Engine interface {
	// Opens or creates a database at the resource path
	Open(resource string) error

	// Closes and releases the database resource
	Close() error

	// Make/Get a handle for a table.
	CreateTable(table string) int32

	// Records the table schema for named table
	// Restructuring existing data
	SyncTable(table int32, schema *TableSchemaStructure) error

	Schema(table int32) *TableSchemaStructure

	NewTransaction() (Transaction, error)

	// Release transaction without committing
	Release(transaction Transaction) error

	// Persists the modified state in Transaction to the engine's storage.
	Commit(transaction Transaction) error

	// Transactionless insertion of records
	Insert(table int32, records []Record) error

	// Transactionless count, returning number of records matched
	Count(table int32, expr *query.Expression) (count uint64, err error)

	// Transactionless query, returning matched records
	Query(table int32, expr *query.Expression) (records []Record, err error)

	// Transactionless update, modifying all matched records to have contents of Record
	Update(table int32, expr *query.Expression, columns []int, values []any) (affected_rows uint64, err error)

	// Transactionless delete, returning number of affected rows
	Delete(table int32, expr *query.Expression) (uint64, error)

	// Transactionless iteration through entire table
	Iterate(table int32, iteration Iteration) (err error)
}

// Transaction represents the state of the Engine and all its tables as it was before NewTransaction was called.
// It also contains information representing isolated modifications to the engine's state.
type Transaction interface {
	Count(table int32, expr *query.Expression) (count uint64, err error)
	Query(table int32, expr *query.Expression) (records []Record, err error)
	Insert(table int32, records []Record) error
	Update(table int32, expr *query.Expression, columns []int, values []any) (affected_rows uint64, err error)
	Delete(table int32, expr *query.Expression) (deleted uint64, err error)
	Iterate(table int32, iteration Iteration) (err error)
}

type Record []any
