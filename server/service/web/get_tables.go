package web

import (
	"net/http"

	"github.com/Gophercraft/phylactery/server/service/web/models"
)

func (service *Service) handle_get_tables(rw http.ResponseWriter, r *http.Request) {
	respond(rw, http.StatusOK, &models.Tables{
		Tables: service.db.Tables(),
	})
}
