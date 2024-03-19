package query

// Matches if multiple conditions are true
func Or(conditions ...Condition) Condition {
	return Condition{
		Type:      Condition_Or,
		Parameter: conditions,
	}
}
