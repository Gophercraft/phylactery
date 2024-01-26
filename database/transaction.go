package database

import (
	"github.com/Gophercraft/phylactery/database/storage"
)

type Transaction struct {
	container           *Container
	storage_transaction storage.Transaction
}

func (container *Container) NewTransaction() (tx *Transaction, err error) {
	tx = new(Transaction)
	tx.container = container
	tx.storage_transaction, err = container.engine.NewTransaction()
	return
}

func (container *Container) Commit(tx *Transaction) error {
	return container.engine.Commit(tx.storage_transaction)
}
