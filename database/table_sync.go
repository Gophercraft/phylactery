package database

import (
	"fmt"
	"reflect"

	"github.com/Gophercraft/phylactery/database/storage"
)

func (table *Table) Schema() *storage.TableSchemaStructure {
	return table.container.engine.Schema(table.table)
}

func (table *Table) SyncSchema(schema *storage.TableSchemaStructure) error {
	// Tell engine to update the underlying table storage (may be quite slow)
	return table.container.engine.SyncTable(table.table, schema)
}

// Sync schematizes a struct type and applies that schema to the underlying table.
// Warning: may be slow.
func (table *Table) Sync(prototype any) error {
	structure_type := reflect.TypeOf(prototype)
	// Typically you would pass a pointer of an empty struct here.
	// That's what we did when we were using Xorm ¯\_(ツ)_/¯
	if structure_type.Kind() == reflect.Pointer {
		structure_type = structure_type.Elem()
	}
	// but if that isn't pointing to a struct, that's an error case
	if structure_type.Kind() != reflect.Struct {
		return fmt.Errorf("passed structure type isn't a structure: '%s'", structure_type.String())
	}
	// Schematize (transform Go type into a persistent format)
	schematized_structure, err := storage.SchematizeStructureType(structure_type)
	if err != nil {
		return err
	}
	// Sync
	return table.SyncSchema(schematized_structure)
}
