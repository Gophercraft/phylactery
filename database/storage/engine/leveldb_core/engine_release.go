package leveldb_core

import "github.com/Gophercraft/phylactery/database/storage"

func (engine *engine) Release(tx storage.Transaction) error {
	transaction := tx.(*transaction)
	transaction.snapshot.Release()
	return nil
}
