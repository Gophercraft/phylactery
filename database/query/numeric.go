package query

import "time"

type Numeric interface {
	~int | ~int8 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64 | time.Time
}

type Integer interface {
	~int | ~int8 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64
}
