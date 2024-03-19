package leveldb_core

import (
	"github.com/Gophercraft/phylactery/database/query"
	"github.com/syndtr/goleveldb/leveldb"
)

func (engine *engine) Count(table_id int32, expr *query.Expression) (count uint64, err error) {
	var table *table

	table, err = engine.get_table(table_id)
	if err != nil {
		return
	}

	if len(expr.Conditions) == 0 {
		table.lock_info()
		count = table.info.rows
		table.unlock_info()
		return
	}

	var snap *leveldb.Snapshot
	snap, err = engine.db.GetSnapshot()
	if err != nil {
		return
	}

	count, err = engine.count_match(snap, table_id, &table.info.schema, expr)
	snap.Release()
	return
}
