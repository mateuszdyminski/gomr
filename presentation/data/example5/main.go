package main

import (
	"plugin"
)

func main() {
	lib, err := plugin.Open("printer.so")
	if err != nil {
		panic(err)
	}

	p, err := lib.Lookup("Print")
	if err != nil {
		panic(err)
	}

	printerF := p.(func(string))

	printerF("Hello Wroclaw")
}
