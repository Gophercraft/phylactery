package query

type ConditionType uint8

const (
	Condition_Equals ConditionType = iota
	Condition_GreaterThan
	Condition_LessThan
	Condition_LessThanOrEqual
	Condition_GreaterThanOrEqual
	Condition_RegularExpression
	Condition_Not
	Condition_Or
	Condition_BitwiseAND
)

type Condition struct {
	ColumnName string
	Column     int
	Type       ConditionType
	Parameter  any
}
