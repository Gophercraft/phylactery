package web

import (
	"fmt"
	"net/http"

	"github.com/Gophercraft/phylactery/database/storage"
	"github.com/Gophercraft/phylactery/server/service/web/models"
)

// func convert_json_isomorph(column_schema *storage.TableSchemaColumn, json_column_value any) (column_value any, err error) {
// 	return
// }

func convert_json_isomorph_column_values(schema *storage.TableSchemaStructure, json_column_names []string, json_column_values []any) (column_values []any, err error) {
	if len(json_column_names) != len(json_column_values) {
		err = fmt.Errorf("invalid number of column values for column names")
		return
	}

	column_values = make([]any, len(json_column_values))

	for json_column_name_index, json_column_name := range json_column_names {
		var column_index int = -1
		for i, schema_column := range schema.Columns {
			if schema_column.Name == json_column_name {
				column_index = i
				break
			}
		}
		if column_index == -1 {
			err = fmt.Errorf("schema has no member named %s", json_column_name)
			return
		}

		column_schema := &schema.Columns[column_index]
		column_values[json_column_name_index], err = convert_json_column_isomorph_value(column_schema, json_column_values[json_column_name_index])
		if err != nil {
			return
		}

	}

	return
}

func (service *Service) handle_post_table_update(rw http.ResponseWriter, r *http.Request) {
	table_name := r.PathValue("table_name")
	table_schema := service.db.TableSchema(table_name)
	if table_schema == nil {
		respond_error(rw, http.StatusBadRequest, fmt.Errorf("no schema for table %s", table_name))
		return
	}

	var table_update models.TableUpdate
	err := read_request(r, &table_update)
	if err != nil {
		respond_error(rw, http.StatusBadRequest, err)
		return
	}

	expression, err := convert_json_expression(table_schema, &table_update.Query)
	if err != nil {
		respond_error(rw, http.StatusBadRequest, err)
		return
	}

	column_values, err := convert_json_isomorph_column_values(table_schema, table_update.ColumnNames, table_update.ColumnValues)
	if err != nil {
		respond_error(rw, http.StatusBadRequest, err)
		return
	}

	table := service.db.Table(table_name)

	updated, err := table.Query(expression).Columns(table_update.ColumnNames...).UpdateColumns(column_values...)
	if err != nil {
		respond_error(rw, http.StatusBadRequest, err)
		return
	}

	respond(rw, http.StatusOK, &models.TableUpdateResponse{
		Updated: updated,
	})
}
