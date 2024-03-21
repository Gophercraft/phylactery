package models

import "github.com/Gophercraft/phylactery/database/storage"

type Tables struct {
	Tables map[string]int32 `json:"tables"`
}

type Error struct {
	Error string
}

type MappedRecords struct {
	Records []storage.Record `json:"records"`
}
