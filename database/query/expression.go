package query

type Expression struct {
	Conditions         []Condition
	Sort               bool
	OrderByColumnIndex int
	Descending         bool
	Limit              uint64 // zero if no limit
}
