package leveldb_core

import (
	"github.com/Gophercraft/phylactery/database/query"
)

func (transaction *transaction) Update(table_id int32, expr *query.Expression, columns []int, values []any) (rows_affected uint64, err error) {
	var (
		table *table
	)
	table, err = transaction.engine.get_table(table_id)
	if err != nil {
		return
	}

	rows, row_IDs, err := transaction.engine.presort_query_full(transaction.snapshot, table_id, expr)
	if err != nil {
		return
	}

	rows_affected, err = update_records(table, expr, transaction.snapshot, &transaction.batch, rows, row_IDs, columns, values)
	if err != nil {
		return
	}

	return

}
