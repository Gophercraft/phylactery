package query

func Lte[T Numeric](column_name string, rh T) Condition {
	var condition Condition
	condition.ColumnName = column_name
	condition.Type = Condition_LessThanOrEqual
	condition.Parameter = rh
	return condition
}
