package main

import (
	"plugin"
)

func main() {
	lib, err := plugin.Open("fancy/fancy-printer.so")
	if err != nil {
		panic(err)
	}
	fPrinter, err := lib.Lookup("Print")
	if err != nil {
		panic(err)
	}
	printerF := fPrinter.(func(string))
	printerF("Hello Wroclaw")

	lib2, err := plugin.Open("ugly/ugly-printer.so")
	if err != nil {
		panic(err)
	}
	uPrinter, err := lib2.Lookup("Print")
	if err != nil {
		panic(err)
	}
	uPrinterF := uPrinter.(func(string))
	uPrinterF("Hello Wroclaw")
}
