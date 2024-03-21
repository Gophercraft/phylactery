package web

import (
	"fmt"
	"net/http"

	"github.com/Gophercraft/phylactery/server/service/web/models"
)

func (service *Service) handle_get_table_schema(rw http.ResponseWriter, r *http.Request) {
	table_name := r.PathValue("table_name")
	table_schema := service.db.TableSchema(table_name)
	if table_schema == nil {
		respond_error(rw, http.StatusBadRequest, fmt.Errorf("no schema for table %s", table_name))
		return
	}

	respond(rw, http.StatusOK, &models.TableSchema{
		Schema: table_schema,
	})
}
