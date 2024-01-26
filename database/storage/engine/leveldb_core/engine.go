package leveldb_core

import (
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
)

type engine struct {
	tables       []*table
	guard_tables sync.Mutex
	db           *leveldb.DB
}
