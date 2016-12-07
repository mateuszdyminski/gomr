package main

import (
	"fmt"
	"github.com/mateuszdyminski/gomr/mapreduce"
	"strconv"
	"strings"
)

func main() {}

// MrImpl implements the gomr MapReduce interface.
type MrImpl struct{}

// Map analyzes the each line of the input file and returns the number of occurrences of number in lotto draw.
// Example of the line:
// 1. 27.01.1957 8,12,31,39,43,45
func (mr MrImpl) Map(key, value string) (result []mapreduce.KeyValue) {
	vals := strings.Split(value, " ")
	if len(vals) != 3 {
		return
	}

	numbers := strings.Split(vals[2], ",")
	if len(numbers) != 6 {
		fmt.Printf("wrong lotto results format: %s\n", vals[2])
		return
	}

	result = make([]mapreduce.KeyValue, 0, 6)
	for _, w := range numbers {
		result = append(result, mapreduce.KeyValue{Key: w, Value: strconv.Itoa(1)})
	}
	return
}

// Reduce calculates the number of particular word(key).
func (mr MrImpl) Reduce(key string, values []string) string {
	counter := 0
	for _, v := range values {
		val, err := strconv.Atoi(v)
		if err != nil {
			continue
		}
		counter += val
	}

	return fmt.Sprintf("%d", counter)
}

// Impl exports the implementation of MapReduce to be available for plugin.Lookup.
var Impl mapreduce.MapReduce = MrImpl{}