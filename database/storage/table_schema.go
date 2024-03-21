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
	Index         bool // Index may or may not speed up lookups.
	Exclusive     bool // When used with Index, Exclusive means that there can only be one index per instance of this column's value
	AutoIncrement bool // Means that the column is an integer that increases
	Name          string
	Tag           uint32
	Size          int32
	Kind          TableSchemaColumnKind
	Members       []TableSchemaColumn
}

type TableSchemaStructure struct {
	Columns []TableSchemaColumn
}

type TableSchemaRow []TableSchemaColumn
