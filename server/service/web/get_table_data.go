package web

import (
	"fmt"
	"net/http"

	"github.com/Gophercraft/phylactery/database/storage"
	"github.com/Gophercraft/phylactery/database/storage/record"
)

func (service *Service) handle_get_table_data(rw http.ResponseWriter, r *http.Request) {
	// table_id, err := strconv.ParseInt(r.PathValue("table_id"), 10, 32)
	// if err != nil {
	// 	respond_error(rw, http.StatusBadRequest, err)
	// 	return
	// }

	table_name := r.PathValue("table_name")
	schema := service.db.TableSchema(table_name)
	if schema == nil {
		respond_error(rw, http.StatusBadRequest, fmt.Errorf("no schema for table %s", table_name))
		return
	}

	rw.WriteHeader(200)

	encoder := record.NewJSONEncoder(rw)

	if err := encoder.EncodeSchemaHeader(schema); err != nil {
		panic(err)
		return
	}

	table := service.db.Table(table_name)

	table.Iterate(func(r storage.Record) (c bool) {
		err := encoder.EncodeRecord(r)
		if err != nil {
			panic(err)
			return false
		}

		return true
	})
}
