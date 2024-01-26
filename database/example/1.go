package main

// Person can change h
type Person struct {
	// Because this field is indexed, the fields will be put into a separate INDEX file.
	ID        uint64    `db:"1:index,exclusive"`
	Name      string    `db:"2:index"`
	Qualities []Quality `db`
}

type Quality struct {
	Kindness float32 `db:"1"`
	Evil     float32 `db:"2"`
	Fear     float64 `db:"3"`
}

func main() {
	// Create new database or load existing database container.
	db, err := database.Open("path/to/folder", database.OpenDefault)
	if err != nil {
		panic(err)
	}

	// Delete all content in the table Person
	table := db.Table("Person")
	table.DeleteAll()

	// Creates or reformats a db table with default characteristics
	// (long term storage)
	// Take care how you sync, removing a field from a struct may cause its contents to be inaccessible
	if err := table.Sync(new(Person)); err != nil {
		panic(err)
	}

	table.Insert(&Person{
		ID:   1,
		Name: "Numba One",
	})

	// Insert records into table

	// Read all rows into array
	var people []*Person
	// Select builds a database.Selector
	// Which has SQL-like query methods
	table.GetAll(&people)

	var person *Person
	db.Table("Person").Where(query.Eq("ID", uint64(1))).GetResult(&person)

	var people []Person

}
