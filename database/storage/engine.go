package storage

import "github.com/Gophercraft/phylactery/database/query"

type Iteration func(record Record) bool

type Engine interface {
	// Opens or creates a database at the resource path
	Open(resource string) error

	// Make/Get a handle for a table.
	CreateTable(table string) int32

	// Records the table schema for named table
	// Restructuring existing data
	SyncTable(table int32, schema *TableSchemaStructure) error

	Schema(table int32) *TableSchemaStructure

	NewTransaction() (Transaction, error)

	// Persists the modified state in Transaction to the engine's storage.
	Commit(transaction Transaction) error

	// Transactionless insert
	Insert(table int32, records []Record) error

	// Transactionless query
	Query(table int32, expr *query.Expression) (records []Record, err error)

	// Transactionless delete
	Delete(table int32, expr *query.Expression) (uint64, error)
}

// Transaction represents the state of the Engine and all its tables as it was before NewTransaction was called.
// It also contains information representing isolated modifications to the engine's state.
type Transaction interface {
	Query(table int32, expr *query.Expression) (records []Record, err error)
	Insert(table int32, records []Record) error
	Delete(table int32, expr *query.Expression) (deleted uint64, err error)
}

type Record []any
