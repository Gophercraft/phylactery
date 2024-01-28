package leveldb_core

import (
	"bytes"
	"encoding/binary"
	"io"
	"sort"

	"github.com/Gophercraft/phylactery/database/storage"
)

const (
	column_flag_exclusive = 1 << iota
	column_flag_index
	column_flag_auto_increment
)

const (
	table_info_flag_has_schema = 1 << iota
)

const (
	max_depth int = 0xFFFF
)

var byteorder = binary.LittleEndian

type table_counters map[uint32]uint64

type table_info struct {
	name                    string
	rows                    uint64
	flag                    uint64
	record_ID_counter       uint64
	schema                  storage.TableSchemaStructure
	auto_increment_counters table_counters
}

func decode_table_schema_column(buffer *bytes.Buffer, column *storage.TableSchemaColumn, depth int) error {
	if depth > max_depth {
		panic("max depth exceeded")
	}

	var column_kind uint8
	var column_size int32
	var column_flag uint32
	var column_tag uint32
	var column_name_size uint32
	var column_num_members uint32
	// Read column kind
	if err := binary.Read(buffer, byteorder, &column_kind); err != nil {
		return err
	}
	// Read column size
	if err := binary.Read(buffer, byteorder, &column_size); err != nil {
		return err
	}
	// Read column tag
	if err := binary.Read(buffer, byteorder, &column_tag); err != nil {
		return err
	}
	// Read column flag
	if err := binary.Read(buffer, byteorder, &column_flag); err != nil {
		return err
	}
	// Read column name size
	if err := binary.Read(buffer, byteorder, &column_name_size); err != nil {
		return err
	}
	// Read number of column members
	if err := binary.Read(buffer, byteorder, &column_num_members); err != nil {
		return err
	}
	// Read column name data
	column_name_bytes := make([]byte, column_name_size)
	if _, err := buffer.Read(column_name_bytes); err != nil {
		return err
	}
	// Build members
	column_members := make([]storage.TableSchemaColumn, column_num_members)
	// Read members
	for column := 0; column < int(column_num_members); column++ {
		if err := decode_table_schema_column(buffer, &column_members[column], depth+1); err != nil {
			return err
		}
	}

	// Build column structure
	column.Kind = storage.TableSchemaColumnKind(column_kind)
	column.Size = column_size
	column.Exclusive = column_flag&column_flag_exclusive != 0
	column.Index = column_flag&column_flag_index != 0
	column.AutoIncrement = column_flag&column_flag_auto_increment != 0
	column.Name = string(column_name_bytes)
	column.Members = column_members
	column.Tag = column_tag

	return nil
}

func decode_table_info(data []byte, info *table_info) (err error) {
	buffer := bytes.NewBuffer(data)

	// Read table name string
	var namelen uint32
	var name []byte
	if err := binary.Read(buffer, byteorder, &namelen); err != nil {
		return err
	}
	name = make([]byte, namelen)
	if _, err := io.ReadFull(buffer, name[:]); err != nil {
		return err
	}
	info.name = string(name)

	// Read number of rows
	if err := binary.Read(buffer, byteorder, &info.rows); err != nil {
		return err
	}
	// Read flag
	if err := binary.Read(buffer, byteorder, &info.flag); err != nil {
		return err
	}
	// Read record ID counter
	if err := binary.Read(buffer, byteorder, &info.record_ID_counter); err != nil {
		return err
	}

	// Read auto-increment counters
	auto_increment_counters, err := decode_table_counters(buffer)
	if err != nil {
		return err
	}
	info.auto_increment_counters = auto_increment_counters

	// Read schema
	var number_of_columns uint32
	if err := binary.Read(buffer, byteorder, &number_of_columns); err != nil {
		return err
	}
	info.schema.Columns = make([]storage.TableSchemaColumn, number_of_columns)
	for i := range info.schema.Columns {
		if err := decode_table_schema_column(buffer, &info.schema.Columns[i], 0); err != nil {
			return err
		}
	}

	return nil
}

func decode_table_counters(buffer *bytes.Buffer) (table_counters, error) {
	var map_len uint32
	if err := binary.Read(buffer, byteorder, &map_len); err != nil {
		return nil, err
	}
	tags := make([]uint32, map_len)
	counters := make([]uint64, map_len)
	if err := binary.Read(buffer, byteorder, tags); err != nil {
		return nil, err
	}
	if err := binary.Read(buffer, byteorder, counters); err != nil {
		return nil, err
	}
	table_counters := make(table_counters, map_len)
	for i := range tags {
		table_counters[tags[i]] = counters[i]
	}
	return table_counters, nil
}

func encode_table_schema_column(buffer *bytes.Buffer, column *storage.TableSchemaColumn, depth int) error {
	if depth > max_depth {
		panic("max depth exceeded")
	}
	var column_kind = uint8(column.Kind)
	var column_size = column.Size
	var column_tag = column.Tag
	var column_flag uint32
	if column.Exclusive {
		column_flag |= column_flag_exclusive
	}
	if column.Index {
		column_flag |= column_flag_index
	}
	if column.AutoIncrement {
		column_flag |= column_flag_auto_increment
	}
	var column_name_bytes = []byte(column.Name)
	var column_name_size = uint32(len(column_name_bytes))
	var column_num_members = uint32(len(column.Members))
	// Write column kind
	if err := binary.Write(buffer, byteorder, column_kind); err != nil {
		return err
	}
	// Write column size
	if err := binary.Write(buffer, byteorder, column_size); err != nil {
		return err
	}
	// Write column tag
	if err := binary.Write(buffer, byteorder, column_tag); err != nil {
		return err
	}
	// Write column flag
	if err := binary.Write(buffer, byteorder, column_flag); err != nil {
		return err
	}
	// Write column name size
	if err := binary.Write(buffer, byteorder, column_name_size); err != nil {
		return err
	}
	// Write column num members
	if err := binary.Write(buffer, byteorder, column_num_members); err != nil {
		return err
	}
	// Write column name bytes
	if _, err := buffer.Write(column_name_bytes); err != nil {
		return err
	}
	for i := 0; i < int(column_num_members); i++ {
		if err := encode_table_schema_column(buffer, &column.Members[i], depth+1); err != nil {
			return err
		}
	}
	return nil
}

func encode_table_counters(buffer *bytes.Buffer, table_counters table_counters) error {
	// Sort the map
	var tags = make([]uint32, len(table_counters))
	for tag := range table_counters {
		tags = append(tags, tag)
	}
	sort.Slice(tags, func(i, j int) bool {
		return tags[i] < tags[j]
	})

	// Write the actual map
	// Write tags first
	var num_tags = uint32(len(tags))
	if err := binary.Write(buffer, byteorder, num_tags); err != nil {
		return err
	}
	if err := binary.Write(buffer, byteorder, tags); err != nil {
		return err
	}
	// Write counters
	for _, tag := range tags {
		counter := table_counters[tag]
		if err := binary.Write(buffer, byteorder, counter); err != nil {
			return err
		}
	}

	return nil
}

func encode_table_info(info *table_info) ([]byte, error) {
	// Write table name string
	buffer := new(bytes.Buffer)
	if err := binary.Write(buffer, byteorder, uint32(len(info.name))); err != nil {
		return nil, err
	}
	if _, err := buffer.Write([]byte(info.name)); err != nil {
		return nil, err
	}

	// Write number of rows
	if err := binary.Write(buffer, byteorder, &info.rows); err != nil {
		return nil, err
	}
	// Write flags
	if err := binary.Write(buffer, byteorder, &info.flag); err != nil {
		return nil, err
	}
	// Write record ID counter
	if err := binary.Write(buffer, byteorder, &info.record_ID_counter); err != nil {
		return nil, err
	}
	// Write auto-increment counters
	if err := encode_table_counters(buffer, info.auto_increment_counters); err != nil {
		return nil, err
	}

	// Write schema
	var number_of_columns = uint32(len(info.schema.Columns))
	if err := binary.Write(buffer, byteorder, &number_of_columns); err != nil {
		return nil, err
	}

	// Write column info
	for i := range info.schema.Columns {
		column := &info.schema.Columns[i]
		if err := encode_table_schema_column(buffer, column, 0); err != nil {
			return nil, err
		}
	}

	return buffer.Bytes(), nil
}

func (table *table) lock_info() {
	table.guard_info.Lock()
}

func (table *table) unlock_info() {
	table.guard_info.Unlock()
}
