package cmd

import (
	"fmt"

	"github.com/Gophercraft/phylactery/database"
	"github.com/Gophercraft/phylactery/server"
	"github.com/spf13/cobra"
)

func run(cmd *cobra.Command, args []string) (err error) {
	if len(args) < 1 {
		return fmt.Errorf("not enough arguments")
	}

	var (
		path   string
		listen string
		engine string
	)

	path = args[0]
	listen, err = cmd.Flags().GetString("listen")
	if err != nil {
		return
	}
	engine, err = cmd.Flags().GetString("engine")
	if err != nil {
		return
	}

	db, err := database.Open(path, database.WithEngine(engine))
	if err != nil {
		return
	}

	return server.RunWeb(listen, db)
}
