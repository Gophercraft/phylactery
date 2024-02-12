package leveldb_core

import (
	"github.com/Gophercraft/phylactery/database/query"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func (engine *engine) Update(table_id int32, expr *query.Expression, columns []int, values []any) (rows_affected uint64, err error) {
	var (
		table    *table
		snapshot *leveldb.Snapshot
	)
	table, err = engine.get_table(table_id)
	if err != nil {
		return
	}

	snapshot, err = engine.db.GetSnapshot()
	defer snapshot.Release()
	if err != nil {
		return
	}

	batch := new(leveldb.Batch)

	rows, row_IDs, err := engine.presort_query_full(snapshot, table_id, expr)
	if err != nil {
		return
	}

	rows_affected, err = update_records(table, expr, snapshot, batch, rows, row_IDs, columns, values)
	if err != nil {
		return
	}

	var write_opts opt.WriteOptions

	err = engine.db.Write(batch, &write_opts)
	return
}
