package database

import (
	"fmt"

	"github.com/Gophercraft/phylactery/database/storage"
)

type Options struct {
	Engine string
}

func Default() *Options {
	return &Options{}
}

func Open(path string, options *Options) (container *Container, err error) {
	container = new(Container)

	if options == nil {
		options = Default()
	}

	if options.Engine == "" {
		options.Engine = "leveldb_core"
	}

	container.engine = storage.NewEngine(options.Engine)
	if container.engine == nil {
		err = fmt.Errorf("no storage engine for %s", options.Engine)
		return
	}

	if err = container.engine.Open(path); err != nil {
		return
	}

	return
}
