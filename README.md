# Gophercraft Phylactery

[![Go Reference](https://pkg.go.dev/badge/github.com/Gophercraft/phylactery.svg)](https://pkg.go.dev/github.com/Gophercraft/phylactery)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![Chat on discord](https://img.shields.io/discord/556039662997733391.svg)](https://discord.gg/xPtuEjt)

Phylactery is an embeddable NoSQL database for Go applications.

Currently the only storage engine for Phylactery is based on [LevelDB](https://github.com/syndtr/goleveldb), an efficient key-value store. The API is somewhat inspired by existing Go SQL ORMs.

It's not recommended that you use Phylactery outside of its intended scope of Gophercraft, I cannot guarantee that your data won't be lost.

# Usage

### Defining data

```go

import "github.com/Gophercraft/phylactery/database"

type Record struct {
    // exclusive index means that this is the only instance of the column that is allowed
    ID   uint64 `database:"1:index,exclusive,auto_increment"` 
    // tagged as second field
    Text string `database:"2"` 
}

// ...

db, err := database.Open("<path to database>", database.WithEngine("leveldb_core"))
if err != nil {
    // ...
}

if err := db.Table("Records").Sync(new(Record)); err != nil {
    // ...
}

```

### Insertion of data

```go
records := []Record {
    {
        Text: "One",
    },

    {
        Text: "Two",
    },

    {
        Text: "Three",
    },
}

if err := db.Table("Records").Insert(&records); err != nil {
    // ...
}

// Because of "auto_increment" tag
fmt.Println(records[0].ID) // 1
fmt.Println(records[1].ID) // 2
fmt.Println(records[2].ID) // 3
```

### Querying

Phylactery has a primitive way to query for records using an array of conditions.

```go
import "github.com/Gophercraft/phylactery/database/query"

// Get a single record
var rec Record
db.Table("Records").Where(query.Eq("ID", uint64(2))).Get(&rec)

fmt.Println(rec.Text) // Two

// Find all records, with ID ascending (unconditional query)
var recs []Record
db.Table("Records").Where().OrderBy("ID", false).Find(&recs)
```

### Using transaction

```go
tx, err := db.NewTransaction()
if err != nil {
    // ...
}

if err := tx.Table("Records").Insert(&records); err != nil {
    // ...
} 

if err := db.Commit(tx); err != nil {
    return
}


```
