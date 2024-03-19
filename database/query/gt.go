package query

// Matches if column value is greater than than right-hand parameter rh
func Gt[T Numeric](column_name string, rh T) Condition {
	var condition Condition
	condition.ColumnName = column_name
	condition.Type = Condition_GreaterThan
	condition.Parameter = rh
	return condition
}
