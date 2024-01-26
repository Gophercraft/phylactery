package leveldb_core

import (
	"encoding/binary"
	"fmt"

	"github.com/Gophercraft/phylactery/database/query"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// wipes out an entire database's content - no going back from this
func (engine *engine) mass_delete(table_id int32) (deleted uint64, err error) {
	var snapshot *leveldb.Snapshot
	snapshot, err = engine.db.GetSnapshot()
	if err != nil {
		return
	}

	var table *table
	table, err = engine.get_table(table_id)
	if err != nil {
		return
	}

	table.lock_info()

	// Start reading at the row sector, but stop reading at the start of a new table.
	var mass_range util.Range
	mass_range.Start = make_record_sector_key(table_id, 0)
	mass_range.Limit = make([]byte, 17)
	binary.LittleEndian.PutUint32(mass_range.Limit[0:4], uint32(table_id+1))
	mass_range.Limit[4] = uint8(key_type_table_info)

	var read_options opt.ReadOptions

	var delete_options opt.WriteOptions

	iter := snapshot.NewIterator(&mass_range, &read_options)

	for iter.Next() {
		key := iter.Key()

		key_table := int32(binary.LittleEndian.Uint32(key[0:4]))
		if key_table != table_id {
			iter.Release()
			err = fmt.Errorf("invalid table ID (%d) in iterator. Iterator sorting is messed up", key_table)
			return
		}

		key_type := key_type(key[4])
		switch key_type {
		case key_type_table_record:
			deleted++
			engine.db.Delete(key, &delete_options)
		case key_type_table_column_exclusive_index:
			engine.db.Delete(key, &delete_options)
		case key_type_table_column_repeatable_index:
			engine.db.Delete(key, &delete_options)
		}
	}

	table.info.rows -= deleted

	engine.put_table_info(table_id)

	table.unlock_info()

	iter.Release()
	snapshot.Release()
	return
}

func (engine *engine) delete_specific_records(table_id int32, delete_list []uint64) (deleted uint64, err error) {
	sort_deletelist(delete_list)

	var snapshot *leveldb.Snapshot
	snapshot, err = engine.db.GetSnapshot()
	if err != nil {
		return
	}

	var table *table
	table, err = engine.get_table(table_id)
	if err != nil {
		return
	}

	table.lock_info()

	// Start reading at the row sector, but stop reading at the start of a new table.
	var mass_range util.Range
	mass_range.Start = make_record_sector_key(table_id, 0)
	mass_range.Limit = make([]byte, 17)
	binary.LittleEndian.PutUint32(mass_range.Limit[0:4], uint32(table_id+1))
	mass_range.Limit[4] = uint8(key_type_table_info)

	var read_options opt.ReadOptions

	var delete_options opt.WriteOptions

	iter := snapshot.NewIterator(&mass_range, &read_options)

	for iter.Next() {
		key := iter.Key()
		value := iter.Value()

		key_table := int32(binary.LittleEndian.Uint32(key[0:4]))
		if key_table != table_id {
			iter.Release()
			err = fmt.Errorf("invalid table ID (%d) in iterator. Iterator sorting is messed up", key_table)
			return
		}

		key_type := key_type(key[4])
		switch key_type {
		case key_type_table_record:
			key_record_ID := binary.LittleEndian.Uint64(key[5:13])
			should_delete := found_in_deletelist(delete_list, key_record_ID)
			if should_delete {
				engine.db.Delete(key, &delete_options)
				deleted++
			}
		case key_type_table_column_exclusive_index, key_type_table_column_repeatable_index:
			value_record_ID := binary.LittleEndian.Uint64(value)
			should_delete := found_in_deletelist(delete_list, value_record_ID)
			if should_delete {
				engine.db.Delete(key, &delete_options)
			}
		}
	}

	table.info.rows -= deleted

	engine.put_table_info(table_id)

	table.unlock_info()

	iter.Release()
	snapshot.Release()
	return
}

func (engine *engine) Delete(table_id int32, expr *query.Expression) (deleted uint64, err error) {
	if len(expr.Conditions) == 0 {
		return engine.mass_delete(table_id)
	}

	var snap *leveldb.Snapshot
	snap, err = engine.db.GetSnapshot()
	if err != nil {
		return
	}

	var record_IDs []uint64
	_, record_IDs, err = engine.presort_query_full(snap, table_id, expr)
	if err != nil {
		return
	}

	snap.Release()

	return engine.delete_specific_records(table_id, record_IDs)
}
