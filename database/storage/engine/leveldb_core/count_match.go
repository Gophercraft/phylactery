package leveldb_core

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/Gophercraft/phylactery/database/query"
	"github.com/Gophercraft/phylactery/database/storage"
	"github.com/Gophercraft/phylactery/database/storage/record"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// (SLOW!!!) iterate through all records
func count_match_iterator_all_records(table_id int32, iter iterator.Iterator, schema *storage.TableSchemaStructure, query_expression *query.Expression) (count uint64, err error) {
	for iter.Next() {
		// Get key & value from LevelDB instance
		key := iter.Key()
		value := iter.Value()

		// Sanity check
		// TODO disable once we are certain that the iterator is ordered correctly
		key_table_id := int32(binary.LittleEndian.Uint32(key[0:4]))
		key_type := key_type(key[4])
		if !(key_type == key_type_table_record && key_table_id == key_table_id) {
			panic(fmt.Errorf("invalid record in iterator, there must be a key sorting failure (key type %d, table id %d)", key_type, key_table_id))
		}
		// Unmarshal value from LevelDB into a Record
		var value_record storage.Record
		value_record, err = record.Unmarshal(schema, value)
		if err != nil {
			return
		}

		// Matched = should we add this to the list?
		var matched bool

		// Range through all of the query conditions, rejecting the record if a condition isn't met
		for c := range query_expression.Conditions {
			condition := &query_expression.Conditions[c]

			column := value_record[condition.Column]

			matched, err = query_column_matches_condition(column, &schema.Columns[condition.Column], condition)
			if err != nil {
				return
			}

			if !matched {
				break
			}
		}

		if matched {
			count++
		}
	}

	iter.Release()
	return
}

func (engine *engine) count_match(snap *leveldb.Snapshot, table_id int32, schema *storage.TableSchemaStructure, query_expression *query.Expression) (count uint64, err error) {
	var range_iteration util.Range
	var read_options opt.ReadOptions
	range_iteration.Start = make_record_sector_key(table_id, 0)
	range_iteration.Limit = make_record_sector_key(table_id, math.MaxUint64)

	iter := snap.NewIterator(&range_iteration, &read_options)
	count, err = count_match_iterator_all_records(table_id, iter, schema, query_expression)
	return
}
