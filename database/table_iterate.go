package database

import (
	"fmt"
	"reflect"

	"github.com/Gophercraft/phylactery/database/storage"
)

var (
	record_type    = reflect.TypeFor[storage.Record]()
	iteration_type = reflect.TypeFor[storage.Iteration]()
	bool_type      = reflect.TypeFor[bool]()
)

func validate_iterator_func(iter_func reflect.Value) (err error) {
	iter_func_type := iter_func.Type()

	if iter_func_type.Kind() != reflect.Func {
		return fmt.Errorf("not a function")
	}

	if !(iter_func_type.NumIn() == 1 &&
		iter_func_type.NumOut() == 1 &&
		iter_func_type.Out(0).Kind() == reflect.Bool &&
		iter_func_type.In(0).Kind() == reflect.Pointer) {
		return fmt.Errorf("not a valid function signature %s", iter_func_type)
	}

	return
}

func create_iterator_func(schema *storage.TableSchemaStructure, fn any) (iter_func storage.Iteration, err error) {
	// Begin reflecting on iterator function
	reflect_func := reflect.ValueOf(fn)
	func_type := reflect_func.Type()

	// If function is already a storage iteration
	is_iter_func := func_type.NumIn() == 1 && func_type.NumOut() == 1 && func_type.In(0) == record_type && func_type.Out(0) == bool_type

	if is_iter_func {
		iter_func = reflect_func.Convert(iteration_type).Interface().(storage.Iteration)
		return
	}

	// Ensure function is of the correct type
	if err = validate_iterator_func(reflect_func); err != nil {
		return
	}

	// Create a "cursor" value which stores the current record
	cursor_value_type := reflect_func.Type().In(0)
	cursor_allocator := func() reflect.Value {
		return reflect.New(cursor_value_type.Elem()).Elem()
	}
	cursor_value := cursor_allocator()

	iter_func = func(record storage.Record) bool {
		if err := storage.UnmapReflectValue(record, cursor_value, schema); err != nil {
			panic(err)
		}

		// Call iterator
		reflect_continue := reflect_func.Call([]reflect.Value{cursor_value.Addr()})
		continue_iteration := reflect_continue[0].Bool()

		// Reset cursor value
		cursor_value.Set(cursor_allocator())

		return continue_iteration
	}

	return
}

func (table *Table) Iterate(fn any) error {
	// Get table schema
	schema := table.Schema()
	if schema == nil {
		return fmt.Errorf("cannot iterate without schema")
	}

	// Create an adapted function to use resolve custom record handler into func(storage.Record) bool
	iterator_func, err := create_iterator_func(schema, fn)
	if err != nil {
		return err
	}

	return table.container.engine.Iterate(table.table, iterator_func)
}
