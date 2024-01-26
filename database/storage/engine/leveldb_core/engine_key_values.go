package leveldb_core

import "github.com/syndtr/goleveldb/leveldb/opt"

func (engine *engine) put(key, value []byte) (err error) {
	return engine.db.Put(key, value, nil)
}

func (engine *engine) put_sync(key, value []byte) (err error) {
	var write_opt opt.WriteOptions
	write_opt.Sync = true
	return engine.db.Put(key, value, &write_opt)
}

func (engine *engine) get(key []byte) (value []byte, err error) {
	return engine.db.Get(key, nil)
}
