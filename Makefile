all: test

.PHONY: test

test:
	@go test network/network_test.go