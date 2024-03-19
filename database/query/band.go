package query

// Matches if the result of column value & right-hand mask is non-zero
func BAnd[T Integer](column_name string, mask T) Condition {
	var condition Condition
	condition.ColumnName = column_name
	condition.Type = Condition_BitwiseAND
	condition.Parameter = mask
	return condition
}
