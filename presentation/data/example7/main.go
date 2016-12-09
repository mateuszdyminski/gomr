package main

import (
	"plugin"
)

func main() {
	lib1, err := plugin.Open("printer1.so")
	if err != nil {
		panic(err)
	}

	lib2, err := plugin.Open("printer2.so")
	if err != nil {
		panic(err)
	}

	p1, err := lib1.Lookup("Print")
	if err != nil {
		panic(err)
	}

	p2, err := lib2.Lookup("Print")
	if err != nil {
		panic(err)
	}

	p1.(func(string))(("Hello Wroclaw"))
	p2.(func(string))(("Hello Wroclaw"))
}
