package storage

import (
	"encoding/json"
	"fmt"
)

type TableSchemaColumnKind uint8

const (
	TableSchemaColumnUint TableSchemaColumnKind = iota
	TableSchemaColumnInt
	TableSchemaColumnFloat
	TableSchemaColumnBool
	TableSchemaColumnString
	TableSchemaColumnBytes
	TableSchemaColumnStructure
	TableSchemaColumnArray
	TableSchemaColumnSlice
	TableSchemaColumnMap
	TableSchemaColumnTime
)

var kind_names = []string{
	"uint",
	"int",
	"float",
	"bool",
	"string",
	"bytes",
	"struct",
	"array",
	"slice",
	"map",
	"time",
}

func (kind TableSchemaColumnKind) MarshalJSON() (b []byte, err error) {
	index := int(kind)
	if index > len(kind_names) {
		err = fmt.Errorf("invalid column schema kind %d", kind)
		return
	}

	b, err = json.Marshal(kind_names[kind])
	return
}

func (kind *TableSchemaColumnKind) UnmarshalJSON(b []byte) (err error) {
	var s string
	if err = json.Unmarshal(b, &s); err != nil {
		return
	}

	for index, kind_name := range kind_names {
		if kind_name == s {
			*kind = TableSchemaColumnKind(index)
			return
		}
	}

	err = fmt.Errorf("invalid column schema kind '%s'", s)
	return
}

type TableSchemaColumn struct {
	Index         bool                  `json:"index"`          // Index may or may not speed up lookups.
	Exclusive     bool                  `json:"exclusive"`      // When used with Index, Exclusive means that there can only be one index per instance of this column's value
	AutoIncrement bool                  `json:"auto_increment"` // Means that the column is an integer that increases
	Name          string                `json:"name"`
	Tag           uint32                `json:"tag"`
	Size          int32                 `json:"size"`
	Kind          TableSchemaColumnKind `json:"kind"`
	Members       []TableSchemaColumn   `json:"members"`
}

type TableSchemaStructure struct {
	Columns []TableSchemaColumn `json:"columns"`
}

type TableSchemaRow []TableSchemaColumn
