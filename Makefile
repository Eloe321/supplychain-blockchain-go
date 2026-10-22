.PHONY: build test race vet demo clean
build:
	mkdir -p bin
	go build -o bin/supplychain ./cmd/supplychain
test:
	go test ./...
race:
	go test -race ./...
vet:
	go vet ./...
demo: build
	go run ./cmd/demo --binary bin/supplychain
clean:
	go clean ./...
