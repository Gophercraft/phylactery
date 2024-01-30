package leveldb_core

import (
	"github.com/Gophercraft/phylactery/database/query"
)

func (tx *transaction) Count(table_id int32, expr *query.Expression) (count uint64, err error) {
	var table *table
	if len(expr.Conditions) == 0 {
		table, err = tx.engine.get_table(table_id)
		if err != nil {
			return
		}
		table.lock_info()
		count = table.info.rows
		table.unlock_info()
		return
	}

	count, err = tx.engine.count_match(tx.snapshot, table_id, &table.info.schema, expr)
	return
}
