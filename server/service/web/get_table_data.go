package web

import (
	"fmt"
	"net/http"

	"github.com/Gophercraft/phylactery/server/service/web/models"
)

func (service *Service) handle_get_table_data(rw http.ResponseWriter, r *http.Request) {
	// table_id, err := strconv.ParseInt(r.PathValue("table_id"), 10, 32)
	// if err != nil {
	// 	respond_error(rw, http.StatusBadRequest, err)
	// 	return
	// }

	table_name := r.PathValue("table_name")
	if service.db.TableSchema(table_name) == nil {
		respond_error(rw, http.StatusBadRequest, fmt.Errorf("no schema for table %s", table_name))
		return
	}

	table := service.db.Table(table_name)
	mapped_records, err := table.Where().FindRecords()
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
