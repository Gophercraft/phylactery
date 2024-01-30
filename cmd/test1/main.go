package main

import (
	"fmt"
	"os"

	"github.com/Gophercraft/phylactery/database"
	"github.com/Gophercraft/phylactery/database/query"
	"github.com/davecgh/go-spew/spew"
)

type schema struct {
	ID   uint64 `database:"1:auto_increment"`
	Data string `database:"2"`
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
	}
}
