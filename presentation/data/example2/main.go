package main

import (
	"log"
	"plugin"
)

func main() {
	lib, err := plugin.Open("printer.so")
	if err != nil {
		panic(err)
	}

	printerName, err := lib.Lookup("PrinterName")
	if err != nil {
		panic(err)
	}

	log.Printf("Printer name is: %s \n", *printerName.(*string))
}
