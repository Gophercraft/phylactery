package database

import (
	"fmt"

	"github.com/Gophercraft/phylactery/database/query"
	"github.com/Gophercraft/phylactery/database/storage"
)

func prepare_query_condition(condition *query.Condition, schema *storage.TableSchemaStructure) {
	condition.Column = -1
	for column_index, column := range schema.Columns {
		if column.Name == condition.ColumnName {
			condition.Column = column_index
			break
		}

		if condition.Type == query.Condition_Not {
			prepare_query_condition(condition.Parameter.(*query.Condition), schema)
		}
	}
	// Was ColumnName found in the schema?
	if condition.Column == -1 {
		panic(fmt.Errorf("invalid query.Condition: no column name '%s", condition.ColumnName))
	}
}

func prepare_query_expression(expression *query.Expression, schema *storage.TableSchemaStructure) {
	for i := range expression.Conditions {
		// Look for Column with the supplied name.
		prepare_query_condition(&expression.Conditions[i], schema)
	}
}
