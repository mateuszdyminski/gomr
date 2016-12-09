package main

import (
	"fmt"
	"log"
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

	safeCall(printerF, "Hello Wroclaw")
}

func safeCall(f func(string), value string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("plugin func call panic: %v", r)
			log.Printf("plugin func call panic: %v. recovering", r)
		}
	}()

	f(value)
	return
}
