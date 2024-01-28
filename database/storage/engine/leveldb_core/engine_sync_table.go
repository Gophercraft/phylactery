package leveldb_core

import (
	"reflect"

	"github.com/Gophercraft/phylactery/database/storage"
)

func (engine *engine) reformat_table(table *table, schema *storage.TableSchemaStructure) error {
	// TODO: ensure compatiblity
	return nil
}

func (engine *engine) SyncTable(table_id int32, schema *storage.TableSchemaStructure) error {
	// Get existing table
	table, err := engine.get_table(table_id)
	if err != nil {
		// Fail if no table exists
		return err
	}

	// If table's already existing information doesn't match the new
	// information, we must reformat it
	if !reflect.DeepEqual(*schema, table.info.schema) {
		if err := engine.reformat_table(table, schema); err != nil {
			return err
		}
	}

	table.lock_info()

	table.info.schema = *schema
	table.info.flag |= table_info_flag_has_schema

	if err := engine.put_table_info(table_id); err != nil {
		return err
	}

	table.unlock_info()
	return nil
}
