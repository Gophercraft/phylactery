package leveldb_core

func (table *table) lock_indices() {
	table.guard_index.Lock()
}

func (table *table) unlock_indices() {
	table.guard_index.Unlock()
}
