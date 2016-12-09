package main

import (
	"plugin"
)

func main() {
	lib, err := plugin.Open("/Users/md/workspace/go/src/github.com/mateuszdyminski/gomr/presentation/data/example1/printer.so")
	if err != nil {
		panic(err)
	}

	fPrinter, err := lib.Lookup("Print")
	if err != nil {
		panic(err)
	}

	printerF := fPrinter.(func(string))
	printerF("Hello Wroclaw")
}
