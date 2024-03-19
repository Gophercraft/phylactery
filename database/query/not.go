package query

// Inverts the matching behavior of the supplied condition
func Not(condition Condition) Condition {
	return Condition{
		Type:      Condition_Not,
		Parameter: &condition,
	}
}
