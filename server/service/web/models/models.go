package models

import (
	"encoding/json"

	"github.com/Gophercraft/phylactery/database/storage"
)

type Tables struct {
	Tables map[string]int32 `json:"tables"`
}

type Error struct {
	Error string
}

type MappedRecords struct {
	Records json.RawMessage `json:"records"`
}

type TableSchema struct {
	Schema *storage.TableSchemaStructure `json:"schema"`
}

type QueryCondition struct {
	Type      string `json:"type"`
	Column    string `json:"column"`
	Parameter any    `json:"parameter"`
}

type QueryExpression struct {
	Conditions []QueryCondition
}

type TableQuery struct {
	Query QueryExpression `json:"query"`
}

type TableInsert struct {
	Records json.RawMessage `json:"records"`
}

type TableInsertResponse struct {
	Inserted uint64 `json:"inserted"`
}

type TableDeleteResponse struct {
	Deleted uint64 `json:"deleted"`
}

type TableUpdate struct {
	Query        QueryExpression `json:"query"`
	ColumnNames  []string        `json:"column_names"`
	ColumnValues []any           `json:"column_values"`
}

type TableUpdateResponse struct {
	Updated uint64 `json:"updated"`
}
