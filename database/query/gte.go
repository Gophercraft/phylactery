package query

func Gte[T Numeric](column_name string, rh T) Condition {
	var condition Condition
	condition.ColumnName = column_name
	condition.Type = Condition_GreaterThanOrEqual
	condition.Parameter = rh
	return condition
}
