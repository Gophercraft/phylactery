package leveldb_core

import (
	"fmt"

	"github.com/Gophercraft/phylactery/database/storage"
)

func (transaction *transaction) Iterate(table_id int32, iteration storage.Iteration) (err error) {
	// Get table
	var table *table
	table, err = transaction.engine.get_table(table_id)
	if err != nil {
		return
	}

	// Fail if the table doesn't have a schema
	if table.info.flag&table_info_flag_has_schema == 0 {
		err = fmt.Errorf("cannot Iterate() on table %d '%s' that lacks a schema", table_id, table.info.name)
		return
	}

	// Directly iterate through raw table
	return iterate_full_record_set(table_id, &table.info.schema, transaction.snapshot, iteration)
}
