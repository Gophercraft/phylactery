package server

import (
	"github.com/Gophercraft/phylactery/database"
	"github.com/Gophercraft/phylactery/server/service/web"
)

type Server interface {
	Run(db *database.Container, address string) error
}

func RunWeb(address string, db *database.Container) (err error) {
	service := web.New()
	return service.Run(db, address)
}
