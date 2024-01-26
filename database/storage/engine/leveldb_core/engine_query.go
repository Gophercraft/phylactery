package leveldb_core

import (
	"github.com/Gophercraft/phylactery/database/query"
	"github.com/Gophercraft/phylactery/database/storage"
	"github.com/syndtr/goleveldb/leveldb"
)

func (engine *engine) Query(table_id int32, expr *query.Expression) (records []storage.Record, err error) {
	var snap *leveldb.Snapshot
	snap, err = engine.db.GetSnapshot()
	if err != nil {
		return
	}

	records, err = engine.query_full(snap, table_id, expr)
	snap.Release()
	return
}
