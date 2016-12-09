package main

import (
	"github.com/mateuszdyminski/gomr/presentation/data/example4/printer"
	"log"
)

type PrinterImpl struct{}

func (p PrinterImpl) Print(value string) {
	log.Printf("Fancy logger: %s \n", value)
}

var Impl printer.Printer = PrinterImpl{}
