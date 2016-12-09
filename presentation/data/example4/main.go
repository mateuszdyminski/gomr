package main

import (
	"github.com/mateuszdyminski/gomr/presentation/data/example4/printer"
	"plugin"
)

func main() {
	lib, err := plugin.Open("fancy/fancy-printer.so")
	if err != nil {
		panic(err)
	}
	p, err := lib.Lookup("Impl")
	if err != nil {
		panic(err)
	}
	printerImpl, ok := p.(*printer.Printer)
	if !ok {
		panic("wrong type")
	}

	(*printerImpl).Print("Hello Wroclaw")
}
