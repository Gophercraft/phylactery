package database

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

func (container *Container) TableID(table_id int32) *Table {
	table := new(Table)
	table.container = container
	table.table = table_id
	return table
}
