package query

import "regexp"

func Regex(column_name string, expr string) Condition {
	compiled_pcre, err := regexp.CompilePOSIX(expr)
	if err != nil {
		panic(err)
	}

	return Condition{
		ColumnName: column_name,
		Type:       Condition_RegularExpression,
		Parameter:  compiled_pcre,
	}
}
