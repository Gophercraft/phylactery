package leveldb_core

import (
	"encoding/binary"
	"fmt"

	"github.com/Gophercraft/phylactery/database/query"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func (transaction *transaction) mass_delete(table_id int32) (deleted uint64, err error) {
	// Start reading at the row sector, but stop reading at the start of a new table.
	var mass_range util.Range
	mass_range.Start = make_record_sector_key(table_id, 0)
	mass_range.Limit = make([]byte, 17)
	binary.LittleEndian.PutUint32(mass_range.Limit[0:4], uint32(table_id+1))
	mass_range.Limit[4] = uint8(key_type_table_info)

	var read_options opt.ReadOptions

	iter := transaction.snapshot.NewIterator(&mass_range, &read_options)

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
			transaction.batch.Delete(key)
		case key_type_table_column_exclusive_index:
			transaction.batch.Delete(key)
		case key_type_table_column_repeatable_index:
			transaction.batch.Delete(key)
		}
	}

	transaction.tables[int(table_id)].deleted_rows += deleted

	iter.Release()
	return
}

func (transaction *transaction) delete_specific_records(table_id int32, delete_list []uint64) (deleted uint64, err error) {
	sort_deletelist(delete_list)

	// Start reading at the row sector, but stop reading at the start of a new table.
	var mass_range util.Range
	mass_range.Start = make_record_sector_key(table_id, 0)
	mass_range.Limit = make([]byte, 17)
	binary.LittleEndian.PutUint32(mass_range.Limit[0:4], uint32(table_id+1))
	mass_range.Limit[4] = uint8(key_type_table_info)

	var read_options opt.ReadOptions

	iter := transaction.snapshot.NewIterator(&mass_range, &read_options)

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
				transaction.batch.Delete(key)
				deleted++
			}
		case key_type_table_column_exclusive_index, key_type_table_column_repeatable_index:
			value_record_ID := binary.LittleEndian.Uint64(value)
			should_delete := found_in_deletelist(delete_list, value_record_ID)
			if should_delete {
				transaction.batch.Delete(key)
			}
		}
	}

	transaction.tables[int(table_id)].deleted_rows += deleted

	iter.Release()
	return
}

func (transaction *transaction) Delete(table_id int32, expr *query.Expression) (deleted uint64, err error) {
	if len(expr.Conditions) == 0 {
		return transaction.mass_delete(table_id)
	}

	var record_IDs []uint64
	_, record_IDs, err = transaction.engine.presort_query_full(transaction.snapshot, table_id, expr)
	if err != nil {
		return
	}

	return transaction.delete_specific_records(table_id, record_IDs)
}
