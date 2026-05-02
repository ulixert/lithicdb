.PHONY: all build test test-v test-race test-integration test-integration-race bench lint fmt vet clean proto gen-simd bench-build

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

# Run integration tests
test-integration:
	go test -tags=integration ./node/...

# Run integration tests with race detector
test-integration-race:
	go test -tags=integration -race ./node/...

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

# Generate protobuf code (requires protoc, protoc-gen-go, protoc-gen-go-grpc)
proto:
	protoc --go_out=. --go_opt=paths=source_relative \
	       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
	       proto/theseonpb/theseon.proto

# Build the benchmark.
bench-build:
	go -C benchmarks build -o /tmp/theseon-vector-bench ./vector/
	go -C benchmarks build -o /tmp/theseon-kv-bench    ./kv_single_node/

# Regenerate AMD64 SIMD assembly from the avo source.
# Output is committed alongside the source.
gen-simd:
	go run ./internal/simd/asm/amd64/main.go \
	    -out internal/simd/l2_amd64.s \
	    -stubs internal/simd/l2_amd64.go \
	    -pkg simd

# Remove test artifacts and generated data
clean:
	go clean -testcache
	rm -rf tmp/ data/