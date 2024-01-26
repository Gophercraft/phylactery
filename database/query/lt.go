package query

func Lt[T Numeric](column_name string, rh T) Condition {
	var condition Condition
	condition.ColumnName = column_name
	condition.Type = Condition_LessThan
	condition.Parameter = rh
	return condition
}
