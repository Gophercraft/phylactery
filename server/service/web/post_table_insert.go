package web

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/Gophercraft/phylactery/database/storage"
	"github.com/Gophercraft/phylactery/server/service/web/models"
)

func (service *Service) handle_post_table_insert(rw http.ResponseWriter, r *http.Request) {
	table_name := r.PathValue("table_name")
	table_schema := service.db.TableSchema(table_name)
	if table_schema == nil {
		respond_error(rw, http.StatusBadRequest, fmt.Errorf("no schema for table %s", table_name))
		return
	}

	var table_insert models.TableInsert
	err := read_request(r, &table_insert)
	if err != nil {
		respond_error(rw, http.StatusBadRequest, err)
		return
	}

	var inserted_records_any any
	err = json.Unmarshal([]byte(table_insert.Records), &inserted_records_any)
	if err != nil {
		respond_error(rw, http.StatusBadRequest, err)
		return
	}

	inserted_records_isomorphic, ok := inserted_records_any.([]any)
	if !ok {
		respond_error(rw, http.StatusBadRequest, fmt.Errorf("not an array of records"))
		return
	}

	inserted_records := make([]storage.Record, len(inserted_records_isomorphic))
	for i := range inserted_records_isomorphic {
		inserted_record_isomorphic, ok := inserted_records_isomorphic[i].([]any)
		if !ok {
			respond_error(rw, http.StatusBadRequest, fmt.Errorf("not a record %d", i))
			return
		}
		inserted_records[i], err = convert_json_record_isomorph_value(table_schema.Columns, inserted_record_isomorphic)
		if err != nil {
			return
		}
	}
	table := service.db.Table(table_name)

	err = table.InsertRecords(inserted_records)
	if err != nil {
		respond_error(rw, http.StatusBadRequest, err)
		return
	}

	respond(rw, http.StatusOK, &models.TableInsertResponse{
		Inserted: uint64(len(inserted_records)),
	})
}
