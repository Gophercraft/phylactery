package query

func Not(condition Condition) Condition {
	return Condition{
		Type:      Condition_Not,
		Parameter: &condition,
	}
}
