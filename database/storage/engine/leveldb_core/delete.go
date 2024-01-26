package leveldb_core

import "sort"

func sort_deletelist(delete_list []uint64) {
	sort.Slice(delete_list, func(i, j int) bool {
		return delete_list[i] < delete_list[j]
	})
}

func found_in_deletelist(delete_list []uint64, value uint64) bool {
	i := sort.Search(len(delete_list), func(i int) bool {
		return delete_list[i] >= value
	})

	return i < len(delete_list) && delete_list[i] == value
}
