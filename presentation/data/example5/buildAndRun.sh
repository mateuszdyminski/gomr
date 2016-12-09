#!/usr/bin/env bash

set -v
$GOTIP build -buildmode=plugin -o printer.so .
$GOTIP run main.go
