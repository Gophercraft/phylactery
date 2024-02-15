package database

func (container *Container) Close() (err error) {
	err = container.engine.Close()
	if err == nil {
		container.engine = nil
	}
	return
}
