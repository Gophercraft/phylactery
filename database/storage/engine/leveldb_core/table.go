package leveldb_core

import "sync"

type table struct {
	id          int32
	info        table_info
	guard_info  sync.Mutex
	guard_index sync.Mutex
}
