package query

import "time"

// Constrains types that can be said to be greater or less than other values of the same type
type Numeric interface {
	~int | ~int8 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64 | time.Time
}

// Constrains types that are subject to bitwise operations
type Integer interface {
	~int | ~int8 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64
}
