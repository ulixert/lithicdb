.PHONY: all build test test-v test-race bench lint fmt vet clean

# Default: run all checks
all: fmt vet lint test-race

# Build the project
build:
	go build ./...

# Run tests
test:
	go test ./...

# Run tests with verbose output
test-v:
	go test -v ./...

# Run tests with race detector — catches concurrent access bugs early,
# which matters a lot once you have background compaction and concurrent readers
test-race:
	go test -race ./...

# Run benchmarks (no tests, just benchmarks)
bench:
	go test -bench=. -benchmem -run=^$$ ./...

# Run linter (install: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest)
lint:
	golangci-lint run ./...

# Format all Go files
fmt:
	gofmt -s -w .

# Vet checks for suspicious constructs
vet:
	go vet ./...

# Remove test artifacts and generated data
clean:
	go clean -testcache
	rm -rf tmp/ data/