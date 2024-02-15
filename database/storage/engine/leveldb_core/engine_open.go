package leveldb_core

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// Open a LevelDB instance to use as the storage of this engine.
func (engine *engine) Open(path string) (err error) {
	var options opt.Options
	options.Comparer = new(key_comparator)
	engine.db, err = leveldb.OpenFile(path, &options)
	if err != nil {
		return
	}

	// Load tables
	err = engine.load_tables()
	return
}

func (engine *engine) Close() (err error) {
	err = engine.db.Close()
	return
}
