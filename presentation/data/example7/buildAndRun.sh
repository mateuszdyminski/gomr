#!/usr/bin/env bash

set -v
$GOTIP build -buildmode=plugin -o printer1.so .
$GOTIP build -buildmode=plugin -o printer2.so .
$GOTIP run main.go
