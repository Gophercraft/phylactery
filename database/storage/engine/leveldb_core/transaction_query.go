package leveldb_core

import (
	"github.com/Gophercraft/phylactery/database/query"
	"github.com/Gophercraft/phylactery/database/storage"
)

func (transaction *transaction) Query(table_id int32, expr *query.Expression) (records []storage.Record, err error) {
	snap := transaction.snapshot

	records, err = transaction.engine.query_full(snap, table_id, expr)
	return
}
