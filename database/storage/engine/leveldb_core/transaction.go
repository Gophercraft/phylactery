package leveldb_core

import (
	"github.com/syndtr/goleveldb/leveldb"
)

type tx_modified_table struct {
	created_rows uint64
	deleted_rows uint64
	indexed      bool
}

type transaction struct {
	engine   *engine
	batch    leveldb.Batch
	snapshot *leveldb.Snapshot
	tables   []*tx_modified_table
}
