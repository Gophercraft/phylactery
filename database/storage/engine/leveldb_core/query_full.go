package leveldb_core

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/Gophercraft/phylactery/database/query"
	"github.com/Gophercraft/phylactery/database/storage"
	"github.com/Gophercraft/phylactery/database/storage/record"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func (engine *engine) presort_query_all_rows_unconditionally(snap snapshot, table_id int32, schema *storage.TableSchemaStructure, limit uint64) (records []storage.Record, record_IDs []uint64, err error) {
	var range_iteration util.Range
	var read_options opt.ReadOptions
	range_iteration.Start = make_record_sector_key(table_id, 0)
	range_iteration.Limit = make_record_sector_key(table_id, math.MaxUint64)

	iter := snap.NewIterator(&range_iteration, &read_options)

	limited := limit > 0

	for iter.Next() {
		// Stop iteration once limit is reached.
		if limited && len(records) >= int(limit) {
			break
		}

		// Get key & value from LevelDB instance
		key := iter.Key()
		value := iter.Value()

		// Sanity check
		// TODO disable once we are certain that the iterator is ordered correctly
		key_table_id := int32(binary.LittleEndian.Uint32(key[0:4]))
		key_type := key_type(key[4])
		if !(key_type == key_type_table_record && table_id == key_table_id) {
			panic(fmt.Errorf("invalid record in iterator, there must be a key sorting failure (key type %d, table id %d)", key_type, key_table_id))
		}
		record_ID := binary.LittleEndian.Uint64(key[5:13])

		// Unmarshal value from LevelDB into a Record
		var value_record storage.Record
		value_record, err = record.Unmarshal(schema, value)
		if err != nil {
			return
		}

		records = append(records, value_record)
		record_IDs = append(record_IDs, record_ID)
	}

	iter.Release()
	return
}

// Perform a full query operation (before sorting)
func (engine *engine) presort_query_full(snap snapshot, table_id int32, expr *query.Expression) (records []storage.Record, record_IDs []uint64, err error) {
	// To be queried, the table must first exist
	var table *table
	table, err = engine.get_table(table_id)
	if err != nil {
		return
	}

	if len(expr.Conditions) == 0 {
		// Query all rows without conditions (no conditions are supplied)
		return engine.presort_query_all_rows_unconditionally(snap, table_id, &table.info.schema, expr.Limit)
	}

	if len(expr.Conditions) == 1 {
		// If the primary condition is an exclusive index, it can be accessed directly
		condition := &expr.Conditions[0]
		condition_column := &table.info.schema.Columns[condition.Column]

		if condition.Type == query.Condition_Equals && condition_column.Index && condition_column.Exclusive {
			// Read record ID of of exclusive index
			var result_ID_bytes []byte
			result_ID_bytes, err = snap.Get(make_exclusive_key(table_id, condition_column, condition.Parameter), nil)
			if err != nil {
				// Of course, if the exclusive key is not found, this isn't an error
				// It just means nothing was found.
				if errors.Is(err, leveldb.ErrNotFound) {
					err = nil
				}
				return
			}
			result_ID := binary.LittleEndian.Uint64(result_ID_bytes)

			// Read record using record ID
			var result_bytes []byte
			result_bytes, err = snap.Get(make_record_sector_key(table_id, result_ID), nil)
			if err != nil {
				return
			}
			var result storage.Record
			result, err = record.Unmarshal(&table.info.schema, result_bytes)
			if err != nil {
				return
			}
			record_IDs = append(record_IDs, binary.LittleEndian.Uint64(result_ID_bytes[:]))

			records = append(records, result)
			return
		}
	}

	return engine.query_match_all_records(table_id, snap, &table.info.schema, expr)
}

type query_order_by_sorter struct {
	column     int
	records    []storage.Record
	descending bool
	comparator sorting_comparator_func
}

func (sorter *query_order_by_sorter) Swap(i, j int) {
	ri := sorter.records[i]
	rj := sorter.records[j]
	sorter.records[i] = rj
	sorter.records[j] = ri
}

func (sorter *query_order_by_sorter) Less(i, j int) bool {
	if sorter.descending {
		return sorter.comparator(sorter.records[i][sorter.column], sorter.records[j][sorter.column])
	} else {
		return sorter.comparator(sorter.records[j][sorter.column], sorter.records[i][sorter.column])
	}
}

func (sorter *query_order_by_sorter) Len() int {
	return len(sorter.records)
}

func (engine *engine) query_full(snap snapshot, table_id int32, expr *query.Expression) (records []storage.Record, err error) {
	records, _, err = engine.presort_query_full(snap, table_id, expr)
	if err != nil {
		return
	}

	if expr.Sort {
		schema := engine.Schema(table_id)
		column := schema.Columns[expr.OrderByColumnIndex]
		var sorter query_order_by_sorter
		sorter.descending = expr.Descending
		sorter.records = records
		sorter.column = expr.OrderByColumnIndex
		sorter.comparator = get_comparator_func(column.Kind, column.Size)
		sort.Sort(&sorter)
		return
	}

	return
}
