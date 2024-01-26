package leveldb_core

import (
	"github.com/Gophercraft/phylactery/database/storage"
)

func (engine *engine) NewTransaction() (storage.Transaction, error) {
	tx := new(transaction)
	engine.guard_tables.Lock()
	tx.tables = make([]*tx_modified_table, len(engine.tables))
	engine.guard_tables.Unlock()
	tx.engine = engine
	snapshot, err := engine.db.GetSnapshot()
	if err != nil {
		return nil, err
	}
	tx.snapshot = snapshot
	return tx, nil
}
