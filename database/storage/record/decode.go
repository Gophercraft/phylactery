package record

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/Gophercraft/phylactery/database/storage"
)

type decoder struct {
	reader io.Reader
}

func (decoder *decoder) ReadByte() (byte, error) {
	var buf [1]byte
	_, err := io.ReadFull(decoder.reader, buf[:])
	return buf[0], err
}

func (decoder *decoder) decode_u8() (uint8, error) {
	b, err := decoder.ReadByte()
	return uint8(b), err
}

func (decoder *decoder) decode_i8() (int8, error) {
	b, err := decoder.ReadByte()
	return int8(b), err
}

func (decoder *decoder) decode_wire_type() (wire_type, error) {
	b, err := decoder.ReadByte()
	return wire_type(b), err
}

func (decoder *decoder) decode_uvarint() (uint64, error) {
	return binary.ReadUvarint(decoder)
}

func (decoder *decoder) decode_varint() (int64, error) {
	return binary.ReadVarint(decoder)
}

func (decoder *decoder) decode_string() (str string, err error) {
	var length uint64
	length, err = decoder.decode_uvarint()
	if err != nil {
		return
	}
	data := make([]byte, length)
	if _, err = io.ReadFull(decoder.reader, data); err != nil {
		return
	}
	str = string(data)
	return
}

func (decoder *decoder) decode_slice(column *storage.TableSchemaColumn) (value any, err error) {
	var element_wire_type wire_type
	var slice_length uint64
	element_wire_type, err = decoder.decode_wire_type()
	if err != nil {
		return
	}

	slice_length, err = decoder.decode_uvarint()
	if err != nil {
		return
	}

	record := make(storage.Record, slice_length)
	for i := uint64(0); i < slice_length; i++ {
		record[i], err = decoder.decode_raw_value(element_wire_type, &column.Members[0])
		if err != nil {
			return
		}
	}

	return record, nil
}

func (decoder *decoder) decode_map(map_column *storage.TableSchemaColumn) (map_record storage.Record, err error) {
	key_column := &map_column.Members[0]
	value_column := &map_column.Members[1]
	var key_wire_type wire_type
	var value_wire_type wire_type
	var map_len uint64
	key_wire_type, err = decoder.decode_wire_type()
	if err != nil {
		return
	}
	value_wire_type, err = decoder.decode_wire_type()
	if err != nil {
		return
	}
	map_len, err = decoder.decode_uvarint()
	if err != nil {
		return
	}
	map_record = make(storage.Record, 2)
	map_keys := make(storage.Record, map_len)
	map_values := make(storage.Record, map_len)
	for i := 0; i < int(map_len); i++ {
		map_keys[i], err = decoder.decode_raw_value(key_wire_type, key_column)
		if err != nil {
			return
		}
	}
	for i := 0; i < int(map_len); i++ {
		map_values[i], err = decoder.decode_raw_value(value_wire_type, value_column)
		if err != nil {
			return
		}
	}
	map_record[0] = map_keys
	map_record[1] = map_values
	return
}

func (decoder *decoder) decode_bool() (bool, error) {
	u8, err := decoder.decode_u8()
	if err != nil {
		return false, err
	}
	return u8 == 1, nil
}

func (decoder *decoder) decode_f32() (float32, error) {
	var bytes [4]byte
	if _, err := io.ReadFull(decoder.reader, bytes[:]); err != nil {
		return 0.0, err
	}
	u32 := binary.LittleEndian.Uint32(bytes[:])
	return math.Float32frombits(u32), nil
}

func (decoder *decoder) decode_f64() (float64, error) {
	var bytes [4]byte
	if _, err := io.ReadFull(decoder.reader, bytes[:]); err != nil {
		return 0.0, err
	}
	u64 := binary.LittleEndian.Uint64(bytes[:])
	return math.Float64frombits(u64), nil
}

func (decoder *decoder) decode_time() (t time.Time, err error) {
	var sizebyte uint8
	sizebyte, err = decoder.decode_u8()
	if err != nil {
		return
	}

	size := int(sizebyte)
	time_bytes := make([]byte, size)
	if _, err = io.ReadFull(decoder.reader, time_bytes); err != nil {
		return
	}

	err = t.UnmarshalBinary(time_bytes)
	return
}

func (decoder *decoder) decode_raw_value(wire_type wire_type, value_column *storage.TableSchemaColumn) (value any, err error) {
	switch wire_type {
	// struct
	case wire_type_structure:
		return decoder.decode_structure(value_column.Members)
	case wire_type_string:
		return decoder.decode_string()
	case wire_type_slice:
		return decoder.decode_slice(value_column)
	case wire_type_map:
		return decoder.decode_map(value_column)
	case wire_type_bool:
		return decoder.decode_bool()
	case wire_type_u8:
		return decoder.decode_u8()
	case wire_type_u16:
		var u64 uint64
		u64, err = decoder.decode_uvarint()
		value = uint16(u64)
		return
	case wire_type_u32:
		var u64 uint64
		u64, err = decoder.decode_uvarint()
		value = uint32(u64)
		return
	case wire_type_u64:
		return decoder.decode_uvarint()
	case wire_type_i8:
		// signed int
		return decoder.decode_i8()
	case wire_type_i16:
		var i64 int64
		i64, err = decoder.decode_varint()
		value = int16(i64)
	case wire_type_i32:
		var i64 int64
		i64, err = decoder.decode_varint()
		value = int32(i64)
	case wire_type_i64:
		return decoder.decode_varint()
	case wire_type_f32:
		// floating-point
		return decoder.decode_f32()
	case wire_type_f64:
		return decoder.decode_f64()
	case wire_type_time:
		return decoder.decode_time()
	default:
		return nil, fmt.Errorf("unhandled wire_type %d", wire_type)
	}

	return
}

func (decoder *decoder) decode_structure(columns []storage.TableSchemaColumn) (record storage.Record, err error) {
	var number_of_columns uint64

	//
	number_of_columns, err = decoder.decode_uvarint()
	if err != nil {
		return
	}

	record = make(storage.Record, len(columns))

	for i := uint64(0); i < number_of_columns; i++ {
		var field_tag_u64 uint64
		field_tag_u64, err = decoder.decode_uvarint()
		if err != nil {
			return
		}
		field_tag := uint32(field_tag_u64)

		for column_index := range columns {
			column_schema := &columns[column_index]
			if column_schema.Tag == field_tag {
				// found column index
				var wire_type wire_type
				wire_type, err = decoder.decode_wire_type()
				if err != nil {
					return
				}
				var value any
				value, err = decoder.decode_raw_value(wire_type, column_schema)
				if err != nil {
					return
				}

				record[column_index] = value
				break
			}
		}

	}

	return
}

func Unmarshal(schema *storage.TableSchemaStructure, data []byte) (storage.Record, error) {
	reader := bytes.NewReader(data)
	var decoder decoder
	decoder.reader = reader

	return decoder.decode_structure(schema.Columns)
}
