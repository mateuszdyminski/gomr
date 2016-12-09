#!/usr/bin/env bash

set -v
cd fancy; $GOTIP build -buildmode=plugin -o fancy-printer.so; cd ..
cd ugly; $GOTIP build -buildmode=plugin -o ugly-printer.so; cd ..
$GOTIP run main.go
