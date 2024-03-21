package web

import (
	"net/http"

	"github.com/Gophercraft/phylactery/database"
)

type Service struct {
	db      *database.Container
	web_mux *http.ServeMux
}

func (service *Service) mount_api() {
	service.web_mux.HandleFunc("GET /api/v1/tables", service.handle_get_tables)
	service.web_mux.HandleFunc("GET /api/v1/table/{table_name}/schema", service.handle_get_table_schema)
	service.web_mux.HandleFunc("GET /api/v1/table/{table_name}/data", service.handle_get_table_data)
	service.web_mux.HandleFunc("POST /api/v1/table/{table_name}/update", service.handle_post_table_update)
}

func New() (service *Service) {
	service = new(Service)
	service.web_mux = http.NewServeMux()
	service.mount_api()
	return
}

func (service *Service) Run(db *database.Container, address string) (err error) {
	service.db = db
	return http.ListenAndServe(address, service.web_mux)
}
