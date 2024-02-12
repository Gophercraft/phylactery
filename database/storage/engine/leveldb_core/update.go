package leveldb_core

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/Gophercraft/phylactery/database/query"
	"github.com/Gophercraft/phylactery/database/storage"
	"github.com/Gophercraft/phylactery/database/storage/record"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func update_records(table *table, expr *query.Expression, snap snapshot, batch *leveldb.Batch, rows []storage.Record, row_IDs []uint64, columns []int, values []any) (rows_affected uint64, err error) {
	limited := expr.Limit != 0

	var record_data []byte
	var read_opts opt.ReadOptions

	for row_index, row := range rows {
		if limited && rows_affected >= expr.Limit {
			break
		}

		row_ID := row_IDs[row_index]

		for index, updated_column := range columns {
			schema_column := &table.info.schema.Columns[updated_column]

			if schema_column.AutoIncrement {
				// ignore, it's the job of the database to change this value.
				continue
			}

			prev_column := row[updated_column]
			next_column := values[index]

			// update indices
			if schema_column.Exclusive && schema_column.Index {
				exclusive_key_prev := make_exclusive_key(table.id, schema_column, prev_column)
				exclusive_key_next := make_exclusive_key(table.id, schema_column, next_column)

				// remove index if it already exists
				if _, err = snap.Get(exclusive_key_prev, &read_opts); err == nil {
					batch.Delete(exclusive_key_prev)
				}

				// if new index already exists, then this is an error condition
				if _, err = snap.Get(exclusive_key_next, &read_opts); err == nil {
					err = fmt.Errorf("leveldb_core: exclusive key value %+v already exists", next_column)
					return
				}

				// Put new index
				var record_ID_bytes [8]byte
				binary.LittleEndian.PutUint64(record_ID_bytes[:], row_ID)
				batch.Put(exclusive_key_next, record_ID_bytes[:])
			} else if schema_column.Index {
				// repeatable index
				repeatable_key_prev := make_repeatable_key(table.id, schema_column, row_ID, prev_column)
				repeatable_key_next := make_repeatable_key(table.id, schema_column, row_ID, next_column)

				// remove index pointing to old value
				if _, err = snap.Get(repeatable_key_prev, &read_opts); err == nil {
					batch.Delete(repeatable_key_next)
				}

				// Put new index
				var record_ID_bytes [8]byte
				binary.LittleEndian.PutUint64(record_ID_bytes[:], row_ID)
				batch.Put(repeatable_key_next, record_ID_bytes[:])

				// if new index doesn't already exist, put it there
				if _, err = snap.Get(repeatable_key_next, &read_opts); err != nil && errors.Is(err, leveldb.ErrNotFound) {
					batch.Put(repeatable_key_next, record_ID_bytes[:])
				}

			}

			row[updated_column] = values[index]
		}

		// write updated record

		record_data, err = record.Marshal(&table.info.schema, row)
		if err != nil {
			return
		}

		batch.Put(make_record_sector_key(table.id, row_ID), record_data)

		rows_affected++
	}

	return

}
