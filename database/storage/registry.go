package storage

type NewEngineFunc func() Engine

var registry = make(map[string]NewEngineFunc)

func Register(name string, fn NewEngineFunc) {
	registry[name] = fn
}

func NewEngine(name string) Engine {
	fn := registry[name]
	if fn == nil {
		return nil
	}

	return fn()
}
