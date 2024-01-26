package query

func Gt[T Numeric](column_name string, rh T) Condition {
	var condition Condition
	condition.ColumnName = column_name
	condition.Type = Condition_GreaterThan
	condition.Parameter = rh
	return condition
}
