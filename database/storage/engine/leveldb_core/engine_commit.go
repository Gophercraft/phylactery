package leveldb_core

import (
	"github.com/Gophercraft/phylactery/database/storage"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func (engine *engine) Commit(storage_transaction storage.Transaction) error {
	tx := storage_transaction.(*transaction)

	var write_opts opt.WriteOptions

	// write_opts.Sync = any_indexed

	if err := engine.db.Write(&tx.batch, &write_opts); err != nil {
		return err
	}

	for table_id_int, mod_table := range tx.tables {
		table_id := int32(table_id_int)
		table, err := engine.get_table(table_id)
		if err != nil {
			panic(err)
		}

		table.lock_info()

		table.info.rows += mod_table.created_rows
		table.info.rows -= mod_table.deleted_rows

		if err := engine.put_table_info(table_id); err != nil {
			table.unlock_info()
			return err
		}

		table.unlock_info()
	}

	return nil
}
