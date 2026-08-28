package wal

import "sort"

func SegmentUpperBound(directory, lastSegmentName string) (string, error) {
	filenames, err := walSegmentNames(directory)
	if err != nil {
		return "", err
	}

	sort.Strings(filenames)
	idx := upperBound(filenames, lastSegmentName)
	if idx < len(filenames) {
		return filenames[idx], nil
	}

	return "", nil
}

func SegmentLast(directory string) (string, error) {
	filenames, err := walSegmentNames(directory)
	if err != nil {
		return "", err
	}

	if len(filenames) == 0 {
		return "", nil
	}
	sort.Strings(filenames)

	return filenames[len(filenames)-1], nil
}

func upperBound(array []string, target string) int {
	low, high := 0, len(array)-1

	for low <= high {
		mid := (low + high) / 2
		if array[mid] > target {
			high = mid - 1
		} else {
			low = mid + 1
		}
	}

	return low
}
