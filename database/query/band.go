package query

// Bitwise And
func BAnd[T Integer](column_name string, mask T) Condition {
	var condition Condition
	condition.ColumnName = column_name
	condition.Type = Condition_BitwiseAND
	condition.Parameter = mask
	return condition
}
