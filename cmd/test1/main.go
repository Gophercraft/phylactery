package main

import (
	"fmt"
	"os"
	"time"

	"github.com/Gophercraft/phylactery/database"
	"github.com/Gophercraft/phylactery/database/query"
	"github.com/davecgh/go-spew/spew"
)

type schema struct {
	ID   uint64    `database:"1:auto_increment"`
	Data string    `database:"2"`
	Time time.Time `database:"3"`
}

var (
	db *database.Container
)

func open_db(path string) {
	var err error
	db, err = database.Open(path, nil)
	if err != nil {
		panic(err)
	}
}

func create_db() {
	if err := db.Table("rec").Sync(new(schema)); err != nil {
		panic(err)
	}
}

func insert_rec_db(data string) {
	if err := db.Table("rec").Insert(&schema{
		Data: data,
		Time: time.Now(),
	}); err != nil {
		panic(err)
	}
}

func dump_db() {
	var recs []schema
	if err := db.Table("rec").Where().OrderBy("Data", false).Find(&recs); err != nil {
		panic(err)
	}

	fmt.Println(len(recs), spew.Sdump(recs))
}

func query_section() {
	var recs []schema
	if err := db.Table("rec").Where(query.Gt("ID", uint64(5))).OrderBy("Data", false).Find(&recs); err != nil {
		panic(err)
	}

	fmt.Println(len(recs), spew.Sdump(recs))
}

func delete_db() {
	rows, err := db.Table("rec").Where(query.Lte("ID", uint64(5))).Delete()
	if err != nil {
		panic(err)
	}

	fmt.Println("deleted", rows, "rows")
}

func regex_query() {
	var recs []schema
	if err := db.Table("rec").Where(query.Regex("Data", "x")).Find(&recs); err != nil {
		panic(err)
	}
	fmt.Println(spew.Sdump(recs))
}

func query_tx_insert() {
	tx, err := db.NewTransaction()
	if err != nil {
		panic(err)
	}
	count, err := tx.Table("rec").Where().Count()
	if err != nil {
		panic(err)
	}
	fmt.Println("Current count =>", count)
	if err = tx.Table("rec").Insert(&schema{
		Data: fmt.Sprintf("this was a transaction %d", count),
	}); err != nil {
		panic(err)
	}

	if err = db.Commit(tx); err != nil {
		panic(err)
	}
}

func iterate_db() {
	if err := db.Table("rec").Iterate(func(s *schema) bool {
		fmt.Println("iterate", s.Data, s.ID)
		return true
	}); err != nil {
		panic(err)
	}
}

func txiterate_db() {
	tx, err := db.NewTransaction()
	if err != nil {
		panic(err)
	}

	if err := tx.Table("rec").Iterate(func(s *schema) bool {
		fmt.Println("txiterate", s.Data, s.ID, s.Time)
		return true
	}); err != nil {
		panic(err)
	}

	db.Release(tx)
}

func schema_db() {
	schema := db.Table("rec").Schema()
	fmt.Println(spew.Sdump(schema))
}

func mass_update() {
	var rec schema
	rec.Time = time.Now()
	updated, err := db.Table("rec").Where().Columns("Time").Update(&rec)
	if err != nil {
		panic(err)
	}

	fmt.Println("updated", updated, "recs")
}

func main() {
	if len(os.Args) < 3 {
		return
	}

	open_db(os.Args[2])

	switch os.Args[1] {
	case "create":
		create_db()
	case "dump":
		dump_db()
	case "insert":
		insert_rec_db(os.Args[3])
	case "query":
		query_section()
	case "delete":
		delete_db()
	case "regex":
		regex_query()
	case "txinsert":
		query_tx_insert()
	case "iterate":
		iterate_db()
	case "txiterate":
		txiterate_db()
	case "schema":
		schema_db()
	case "update":
		mass_update()
	}
}
