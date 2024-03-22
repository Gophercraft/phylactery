package storage

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
