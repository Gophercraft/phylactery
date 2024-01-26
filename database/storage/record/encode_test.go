package record_test

import (
	"fmt"
	"testing"

	"github.com/Gophercraft/phylactery/database/storage"
	"github.com/Gophercraft/phylactery/database/storage/engine/leveldb_bin/record"
	"github.com/davecgh/go-spew/spew"
)

var person_schema = &storage.TableSchemaStructure{
	Columns: []storage.TableSchemaColumn{
		{
			Name: "Name",
			Tag:  1,
			Kind: storage.TableSchemaColumnString,
		},

		{
			Name: "Age",
			Tag:  2,
			Kind: storage.TableSchemaColumnUint,
			Size: 64,
		},
	},
}

var food_schema = &storage.TableSchemaStructure{
	Columns: []storage.TableSchemaColumn{
		{
			Name: "Name",
			Tag:  1,
			Kind: storage.TableSchemaColumnString,
		},

		{
			Name: "PH_Value",
			Tag:  2,
			Kind: storage.TableSchemaColumnFloat,
			Size: 64,
		},

		{
			Name: "Descriptions",
			Tag:  3,
			Kind: storage.TableSchemaColumnSlice,
			Members: []storage.TableSchemaColumn{
				{
					Kind: storage.TableSchemaColumnString,
				},
			},
		},
	},
}

func TestRecord(t *testing.T) {
	data, err := record.Marshal(person_schema, storage.Record{
		string("John Doe"),
		uint64(50),
	})

	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(spew.Sdump(data))
}

func TestRecord2(t *testing.T) {
	data, err := record.Marshal(food_schema, storage.Record{
		string("Tomatoes"),
		float64(6.5),
		storage.Record{
			"Bright",
			"Fruity",
			"Savory",
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(spew.Sdump(data))

	fail_cases := []storage.Record{
		{
			string("Ammonia"),
			float64(11.2),
			storage.Record{
				"Poisonous",
				"Not tasty!",
			},
			"This shouldn't be here",
		},

		{
			// wrong type
			storage.Record{},
			float64(-5.0),
			storage.Record{
				"Poisonous",
				"Not tasty!",
			},
		},
	}

	for _, fail_case := range fail_cases {
		_, err = record.Marshal(food_schema, fail_case)
		if err == nil {
			t.Fatal("passed when shouldn't have")
		}
	}

}
