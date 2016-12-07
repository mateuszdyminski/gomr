package main

import (
	"plugin"
	"github.com/mateuszdyminski/gomr/mapreduce"
)

func main() {
	lib, err := plugin.Open("WordCount-1481018883-245106387.so")
	if err != nil {
		panic(err)
	}

	p, err := lib.Lookup("Impl")
	if err != nil {
		panic(err)
	}

	impl, ok := p.(*mapreduce.MapReduce)
	if !ok {
		panic("wrong type")
	}

	(*impl).Map("test", "test")
}
