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

type encoder struct {
	writer io.Writer
}

// encode a variable-length unsigned integer into the output stream.
func (encoder *encoder) encode_uvarint(u64 uint64) error {
	buf := make([]byte, binary.MaxVarintLen64)
	size := binary.PutUvarint(buf, u64)
	_, err := encoder.writer.Write(buf[:size])
	return err
}

// encode a variable-length signed integer into the output stream.
func (encoder *encoder) encode_varint(i64 int64) error {
	buf := make([]byte, binary.MaxVarintLen64)
	size := binary.PutVarint(buf, i64)
	_, err := encoder.writer.Write(buf[:size])
	return err
}

// encode a byte into the output stream
func (encoder *encoder) encode_u8(u8 uint8) error {
	var buf [1]byte
	buf[0] = u8
	_, err := encoder.writer.Write(buf[:])
	return err
}

func (encoder *encoder) encode_boolean(b bool) error {
	if b {
		return encoder.encode_u8(1)
	}

	return encoder.encode_u8(0)
}

func (encoder *encoder) encode_string(str string) error {
	// Encode the length of the string in bytes
	if err := encoder.encode_uvarint(uint64(len(str))); err != nil {
		return err
	}

	// Encode the string's bytes
	if _, err := encoder.writer.Write([]byte(str)); err != nil {
		return err
	}

	return nil
}

func (encoder *encoder) encode_f32(f32 float32) (err error) {
	var bytes [4]byte
	binary.LittleEndian.PutUint32(bytes[:], math.Float32bits(f32))
	_, err = encoder.writer.Write(bytes[:])
	return
}

func (encoder *encoder) encode_f64(f64 float64) (err error) {
	var bytes [8]byte
	binary.LittleEndian.PutUint64(bytes[:], math.Float64bits(f64))
	_, err = encoder.writer.Write(bytes[:])
	return
}

func (encoder *encoder) encode_time(t time.Time) (err error) {
	var time_bytes []byte
	time_bytes, err = t.MarshalBinary()
	if err != nil {
		return
	}
	if err = encoder.encode_u8(uint8(len(time_bytes))); err != nil {
		return
	}
	if _, err = encoder.writer.Write(time_bytes); err != nil {
		return
	}
	return
}

// encode raw value without any prefix
func (encoder *encoder) encode_raw_value(wire_type wire_type, value_column *storage.TableSchemaColumn, value any) (err error) {
	switch wire_type {
	// struct
	case wire_type_structure:
		structure_value, ok := value.(storage.Record)
		if !ok {
			return fmt.Errorf("value %+v should be a storage.Record", value)
		}
		err = encoder.encode_structure(value_column.Members, structure_value)
	case wire_type_string:
		// string
		string_value, ok := value.(string)
		if !ok {
			return fmt.Errorf("value %+v should be a string", string_value)
		}
		err = encoder.encode_string(value.(string))
	case wire_type_map:
		map_value, ok := value.(storage.Record)
		if !ok {
			return fmt.Errorf("value %+v should be a storage.Record", value)
		}
		err = encoder.encode_map(value_column, map_value)
	case wire_type_slice:
		// slice
		slice_value, ok := value.(storage.Record)
		if !ok {
			return fmt.Errorf("value %+v should be a storage.Record", value)
		}
		err = encoder.encode_slice(value_column, slice_value)
	case wire_type_bool:
		// boolean
		bool_value, ok := value.(bool)
		if !ok {
			return fmt.Errorf("value %+v should be a bool", value)
		}
		err = encoder.encode_boolean(bool_value)
	case wire_type_u8:
		// unsigned int
		u8_value, ok := value.(uint8)
		if !ok {
			return fmt.Errorf("value %+v should be a uint8", value)
		}
		err = encoder.encode_u8(u8_value)
	case wire_type_u16:
		u16_value, ok := value.(uint16)
		if !ok {
			return fmt.Errorf("value %+v should be a uint16", value)
		}
		err = encoder.encode_uvarint(uint64(u16_value))
	case wire_type_u32:
		u32_value, ok := value.(uint32)
		if !ok {
			return fmt.Errorf("value %+v should be a uint32", value)
		}
		err = encoder.encode_uvarint(uint64(u32_value))
	case wire_type_u64:
		u64_value, ok := value.(uint64)
		if !ok {
			return fmt.Errorf("value %+v should be a uint64", value)
		}
		err = encoder.encode_uvarint(u64_value)
	case wire_type_i8:
		// signed int
		i8_value, ok := value.(int8)
		if !ok {
			return fmt.Errorf("value %+v should be a int8", value)
		}
		err = encoder.encode_u8(uint8(i8_value))
	case wire_type_i16:
		i16_value, ok := value.(int16)
		if !ok {
			return fmt.Errorf("value %+v should be a int16", value)
		}
		err = encoder.encode_varint(int64(i16_value))
	case wire_type_i32:
		i32_value, ok := value.(int32)
		if !ok {
			return fmt.Errorf("value %+v should be a int32", value)
		}
		err = encoder.encode_varint(int64(i32_value))
	case wire_type_i64:
		i64_value, ok := value.(int64)
		if !ok {
			return fmt.Errorf("value %+v should be a int64", value)
		}
		err = encoder.encode_varint(int64(i64_value))
	case wire_type_f32:
		// floating-point
		f32_value, ok := value.(float32)
		if !ok {
			return fmt.Errorf("value %+v should be a float32", value)
		}
		err = encoder.encode_f32(f32_value)
	case wire_type_f64:
		f64_value, ok := value.(float64)
		if !ok {
			return fmt.Errorf("value %+v should be a float64", value)
		}
		err = encoder.encode_f64(f64_value)
	case wire_type_time:
		time_value, ok := value.(time.Time)
		if !ok {
			return fmt.Errorf("value %+v should be a time.Time", value)
		}
		err = encoder.encode_time(time_value)
	default:
		return fmt.Errorf("unhandled wire_type %d", err)
	}

	return
}

func (encoder *encoder) encode_map(map_column *storage.TableSchemaColumn, column_value storage.Record) error {
	key_column := &map_column.Members[0]
	value_column := &map_column.Members[1]
	keys := column_value[0].(storage.Record)
	values := column_value[1].(storage.Record)

	key_wire_type, err := get_wire_type(key_column.Kind, key_column.Size)
	if err != nil {
		return err
	}
	value_wire_type, err := get_wire_type(value_column.Kind, value_column.Size)
	if err != nil {
		return err
	}
	if err := encoder.encode_u8(uint8(key_wire_type)); err != nil {
		return err
	}
	if err := encoder.encode_u8(uint8(value_wire_type)); err != nil {
		return err
	}
	if err := encoder.encode_uvarint(uint64(len(keys))); err != nil {
		return err
	}
	for _, key := range keys {
		if err := encoder.encode_raw_value(key_wire_type, key_column, key); err != nil {
			return err
		}
	}
	for _, value := range values {
		if err := encoder.encode_raw_value(value_wire_type, value_column, value); err != nil {
			return err
		}
	}
	return nil
}

func (encoder *encoder) encode_slice(slice_column *storage.TableSchemaColumn, column_value storage.Record) error {
	// Encode wire type of slice elements
	element_column := &slice_column.Members[0]
	element_wire_type, err := get_wire_type(element_column.Kind, element_column.Size)
	if err != nil {
		return err
	}
	if err := encoder.encode_u8(uint8(element_wire_type)); err != nil {
		return err
	}

	// Encode the length of the slice
	value_slice_len := len(column_value)
	if err := encoder.encode_uvarint(uint64(value_slice_len)); err != nil {
		return err
	}

	// Encode the array's members as raw values
	for i := 0; i < value_slice_len; i++ {
		if err := encoder.encode_raw_value(element_wire_type, element_column, column_value[i]); err != nil {
			return err
		}
	}

	return nil
}

func (encoder *encoder) encode_structure(schema []storage.TableSchemaColumn, record storage.Record) error {
	schema_len := len(schema)
	if schema_len != len(record) {
		err := fmt.Errorf("schema structure length %d mismatched with record len %d %+v", schema_len, len(record), record)

		panic(err)
		return err
	}

	// Encode length of record
	if err := encoder.encode_uvarint(uint64(schema_len)); err != nil {
		return err
	}

	for i := 0; i < schema_len; i++ {
		schema_field := &schema[i]

		// Encode wire tag
		if err := encoder.encode_uvarint(uint64(schema_field.Tag)); err != nil {
			return err
		}

		// Encode wire type
		wire_type, err := get_wire_type(schema_field.Kind, schema_field.Size)
		if err != nil {
			return err
		}
		if err := encoder.encode_u8(uint8(wire_type)); err != nil {
			return err
		}

		// Encode column
		if err := encoder.encode_raw_value(wire_type, schema_field, record[i]); err != nil {
			return err
		}
	}

	return nil
}

// Marshal a raw Record (variable type values loosely)
func Marshal(schema *storage.TableSchemaStructure, record storage.Record) ([]byte, error) {
	var buffer bytes.Buffer
	var encoder encoder
	encoder.writer = &buffer

	if err := encoder.encode_structure(schema.Columns, record); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}
