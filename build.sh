#!/bin/bash -i
mkdir -p bin
go build -gcflags all=-N consumer.go
go build -gcflags all=-N producer.go

mv consumer ./bin/
mv producer ./bin/
