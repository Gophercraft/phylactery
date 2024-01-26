package leveldb_core

import "github.com/Gophercraft/phylactery/database/storage"

func init() {
	storage.Register("leveldb_core", func() storage.Engine {
		return new(engine)
	})
}
