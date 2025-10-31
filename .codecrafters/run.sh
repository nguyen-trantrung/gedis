#!/bin/bash

set -e
cd /cmd/gedis
exec go run main.go "$@"