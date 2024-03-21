package database

import "github.com/Gophercraft/phylactery/database/storage"

type Table struct {
	container *Container
	table     int32
}

func (container *Container) Table(table_name string) *Table {
	table := new(Table)
	table.container = container
	table.table = container.engine.CreateTable(table_name)
	return table
}

func (container *Container) Tables() (tables map[string]int32) {
	return container.engine.Tables()
}

func (container *Container) TableSchema(table_name string) (record_struct *storage.TableSchemaStructure) {
	tables := container.Tables()
	var table_id int32
	var ok bool
	if table_id, ok = tables[table_name]; !ok {
		return
	}

	return container.engine.Schema(table_id)
}
