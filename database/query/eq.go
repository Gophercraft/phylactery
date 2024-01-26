package query

func Eq(column_name string, parameter any) (condition Condition) {
	condition.Type = Condition_Equals
	condition.ColumnName = column_name
	condition.Parameter = parameter
	return condition
}
