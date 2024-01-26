package leveldb_core

import (
	"github.com/Gophercraft/phylactery/database/storage"
)

func (engine *engine) Schema(table_id int32) *storage.TableSchemaStructure {
	table, err := engine.get_table(table_id)
	if err != nil {
		return nil
	}
	return &table.info.schema
}
