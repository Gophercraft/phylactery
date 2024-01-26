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
	}
}
