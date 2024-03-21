package database

import (
	"fmt"

	"github.com/Gophercraft/phylactery/database/storage"
)

type Option func(opt *Options) error

type Options struct {
	Engine string
}

func WithEngine(engine string) Option {
	return func(opt *Options) error {
		opt.Engine = engine
		return nil
	}
}

func Open(path string, with_options ...Option) (container *Container, err error) {
	container = new(Container)

	var opt Options
	opt.Engine = "leveldb_core"

	for _, option := range with_options {
		if err = option(&opt); err != nil {
			return
		}
	}

	container.engine = storage.NewEngine(opt.Engine)
	if container.engine == nil {
		err = fmt.Errorf("no storage engine for %s", opt.Engine)
		return
	}

	if err = container.engine.Open(path); err != nil {
		return
	}

	return
}
