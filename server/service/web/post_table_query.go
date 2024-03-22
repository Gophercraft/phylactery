package web

import (
	"fmt"
	"net/http"

	"github.com/Gophercraft/phylactery/server/service/web/models"
)

func (service *Service) handle_post_table_query(rw http.ResponseWriter, r *http.Request) {
	table_name := r.PathValue("table_name")
	table_schema := service.db.TableSchema(table_name)
	if table_schema == nil {
		respond_error(rw, http.StatusBadRequest, fmt.Errorf("no schema for table %s", table_name))
		return
	}
	var table_query models.TableQuery

	if err := read_request(r, &table_query); err != nil {
		respond_error(rw, http.StatusBadRequest, err)
		return
	}

	expression, err := convert_json_expression(table_schema, &table_query.Query)
	if err != nil {
		respond_error(rw, http.StatusBadRequest, err)
		return
	}

	table := service.db.Table(table_name)
	mapped_records, err := table.Query(expression).FindRecords()
	if err != nil {
		respond_error(rw, http.StatusBadRequest, err)
		return
	}

	mapped_isomorphic, err := encode_isomorph_records_json(mapped_records)
	if err != nil {
		respond_error(rw, http.StatusBadRequest, err)
		return
	}

	fmt.Println(string(mapped_isomorphic))

	respond(rw, http.StatusOK, &models.MappedRecords{
		Records: mapped_isomorphic,
	})
}
