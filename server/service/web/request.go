package web

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/Gophercraft/phylactery/server/service/web/models"
)

func read_request(r *http.Request, arguments any) (err error) {
	var json_data []byte
	json_data, err = io.ReadAll(r.Body)
	if err != nil {
		return
	}

	err = json.Unmarshal(json_data, arguments)
	return
}

func respond(rw http.ResponseWriter, status int, result any) {
	var json_data []byte
	var err error
	if json_data, err = json.Marshal(result); err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	rw.Header().Set("Content-Type", "application/json; charset=utf-8")
	rw.WriteHeader(status)
	rw.Write(json_data)
}

func respond_error(rw http.ResponseWriter, status int, err error) {
	respond(rw, status, &models.Error{
		Error: err.Error(),
	})
}
