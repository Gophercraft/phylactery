package record

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"time"

	"github.com/Gophercraft/phylactery/database/storage"
)

var bytes_type = reflect.TypeFor[[]byte]()
var time_type = reflect.TypeFor[time.Time]()

func format_float32(x float32) string {
	str, _ := json.Marshal(x)
	return string(str)
}

func format_float64(x float64) string {
	str, _ := json.Marshal(x)
	return string(str)
}

func encode_string_json(writer io.Writer, value string) (err error) {
	_, err = writer.Write([]byte(strconv.Quote(value)))
	return
}

func encode_isomorph_json(writer io.Writer, value any) (err error) {
	rvalue := reflect.ValueOf(value)

	switch rvalue.Type() {
	case bytes_type:
		str := base64.StdEncoding.EncodeToString(value.([]byte))

		return encode_string_json(writer, str)
	case time_type:
		time_value := value.(time.Time)
		time_string := time_value.UTC().Format(time.RFC3339)
		return encode_string_json(writer, time_string)
	}

	switch rvalue.Kind() {
	case reflect.Bool:
		if rvalue.Bool() {
			return encode_string_json(writer, "1")
		} else {
			return encode_string_json(writer, "0")
		}
	case reflect.String:
		return encode_string_json(writer, rvalue.String())
	case reflect.Slice:
		if _, err = writer.Write([]byte{'['}); err != nil {
			return
		}

		ceiling := rvalue.Len() - 1

		for i := 0; i < rvalue.Len(); i++ {
			err = encode_isomorph_json(writer, rvalue.Index(i).Interface())
			if err != nil {
				return
			}

			if i != ceiling {
				if _, err = writer.Write([]byte{','}); err != nil {
					return
				}
			}
		}

		if _, err = writer.Write([]byte{']'}); err != nil {
			return
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return encode_string_json(writer, strconv.FormatUint(rvalue.Uint(), 10))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return encode_string_json(writer, strconv.FormatInt(rvalue.Int(), 10))
	case reflect.Float32:
		return encode_string_json(writer, format_float32(value.(float32)))
	case reflect.Float64:
		return encode_string_json(writer, format_float64(value.(float64)))
	default:
		return fmt.Errorf("value cannot have kind of %s", rvalue.Kind())
	}

	return
}

type JSONEncoder struct {
	encoder *json.Encoder
}

func (j *JSONEncoder) EncodeSchemaHeader(schema *storage.TableSchemaStructure) (err error) {
	err = j.encoder.Encode(schema)
	return
}

func (j *JSONEncoder) EncodeRecord(record storage.Record) (err error) {
	var buf bytes.Buffer

	err = encode_isomorph_json(&buf, record)
	if err != nil {
		return
	}

	raw := json.RawMessage(buf.Bytes())

	err = j.encoder.Encode(raw)
	return
}

func NewJSONEncoder(out io.Writer) (encoder *JSONEncoder) {
	encoder = new(JSONEncoder)
	encoder.encoder = json.NewEncoder(out)
	return
}
