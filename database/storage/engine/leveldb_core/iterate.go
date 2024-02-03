package leveldb_core

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/Gophercraft/phylactery/database/storage"
	"github.com/Gophercraft/phylactery/database/storage/record"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func iterate_full_record_set(table_id int32, schema *storage.TableSchemaStructure, snap snapshot, iteration storage.Iteration) (err error) {
	// setup LevelDB iteration context
	var range_iteration util.Range
	var read_options opt.ReadOptions
	var record_iterator iterator.Iterator
	var record_data storage.Record
	range_iteration.Start = make_record_sector_key(table_id, 0)
	range_iteration.Limit = make_record_sector_key(table_id, math.MaxUint64)
	record_iterator = snap.NewIterator(&range_iteration, &read_options)

	// iterate through record sector
	for record_iterator.Next() {
		record_key := record_iterator.Key()
		record_value := record_iterator.Value()

		// validate key entry
		// uint32(table_id) : uint8(key_type)
		record_table_ID := int32(binary.LittleEndian.Uint32(record_key[0:4]))
		record_key_type := key_type(record_key[4])
		valid_entry := record_table_ID == table_id && record_key_type == key_type_table_record
		if !valid_entry {
			err = fmt.Errorf("invalid record entry table_id(%d) key_type(%d)", record_table_ID, record_key_type)
			break
		}

		// unmarshal record
		record_data, err = record.Unmarshal(schema, record_value)
		if err != nil {
			break
		}

		// run iteration handler
		continue_iterating := iteration(record_data)
		if !continue_iterating {
			break
		}
	}

	record_iterator.Release()
	return
}
