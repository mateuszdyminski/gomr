package main

import (
	"fmt"
	"github.com/mateuszdyminski/gomr/mapreduce"
	"strconv"
	"strings"
	"unicode"
)

func main() {}

// MrImpl implements the gomr MapReduce interface.
type MrImpl struct{}

// Map analyzes the each line of the input file and returns the number of occurrences of word.
func (mr MrImpl) Map(key, value string) (result []mapreduce.KeyValue) {
	isNotLetter := func(r rune) bool { return !unicode.IsLetter(r) }
	words := strings.FieldsFunc(value, isNotLetter)

	result = make([]mapreduce.KeyValue, 0, len(words))
	for _, w := range words {
		result = append(result, mapreduce.KeyValue{Key: w, Value: strconv.Itoa(1)})
	}
	return
}

// Reduce calculates the number of particular work(key).
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