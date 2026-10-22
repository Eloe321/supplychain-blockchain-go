#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
if [ ! -x bin/supplychain ]; then
  mkdir -p bin
  go build -o bin/supplychain ./cmd/supplychain
fi
exec go run ./cmd/demo --binary bin/supplychain "$@"
