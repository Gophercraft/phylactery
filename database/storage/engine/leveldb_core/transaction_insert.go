package leveldb_core

import (
	"encoding/binary"

	"github.com/Gophercraft/phylactery/database/storage"
	"github.com/Gophercraft/phylactery/database/storage/record"
)

func (transaction *transaction) Insert(table_id int32, inserted_records []storage.Record) error {
	table, err := transaction.engine.get_table(table_id)
	if err != nil {
		return err
	}

	mod_table := transaction.tables[int(table_id)]

	if mod_table == nil {
		mod_table = new(tx_modified_table)
		transaction.tables[int(table_id)] = mod_table
	}

	// we need to get new sequences from the table's info counters, so make sure anyone else doesn't try to do the same
	table.lock_info()

	record_ID_counter := table.info.record_ID_counter
	record_IDs := make([]uint64, len(inserted_records))

	for i := range inserted_records {
		// Get ID for this record
		record_ID_counter++

		inserted_record := inserted_records[i]

		for column_index := range table.info.schema.Columns {
			column := &table.info.schema.Columns[column_index]
			if column.AutoIncrement {
				// Apply auto-increment counters to field
				counter := table.info.auto_increment_counters[column.Tag] + 1
				inserted_record[column_index] = counter
				table.info.auto_increment_counters[column.Tag] = counter
			}

			// create an exclusive index for this field
			// table ID:key_type_table_column_exclusive_index:column Tag:field value
			if column.Index && column.Exclusive {
				var record_ID_value [8]byte
				binary.LittleEndian.PutUint64(record_ID_value[:], record_ID_counter)
				// table_values[exclusive_key] => record ID
				transaction.batch.Put(make_exclusive_key(table_id, column, inserted_record[column_index]), record_ID_value[:])
			} else if column.Index {
				// create repeatable index
				var record_ID_value [8]byte
				binary.LittleEndian.PutUint64(record_ID_value[:], record_ID_counter)
				transaction.batch.Put(make_repeatable_key(table_id, column, record_ID_counter, inserted_record[column_index]), record_ID_value[:])
			}
		}

		// Serialize record
		var err error
		var serialized_record []byte
		serialized_record, err = record.Marshal(&table.info.schema, inserted_record)
		if err != nil {
			table.unlock_info()
			return err
		}

		// Increment

		// Store record
		record_key := make_record_sector_key(table_id, record_ID_counter)

		record_IDs[i] = record_ID_counter

		transaction.batch.Put(record_key[:], serialized_record)

		mod_table.created_rows++
	}

	// Rewrite auto-increment counters, as well as updated number of table rows to storage
	table.info.record_ID_counter = record_ID_counter
	if err := transaction.engine.put_table_info(table_id); err != nil {
		return err
	}

	// Allow other callers to modify table info
	table.unlock_info()

	return nil
}
