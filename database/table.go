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
