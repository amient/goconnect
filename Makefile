all: test

.PHONY: test release

test:
	@go test


release: test
	@if [ -z "$(v)" ]; then echo "Usage: make v=Major.Minor.Patch release"; exit 1; fi
	@LANG=en_GB git tag "v$(v)"
	@LANG=en_GB git tag "io/kafka1/v$(v)"
	@LANG=en_GB git tag "io/amqp09/v$(v)"
	@LANG=en_GB git tag "coder/avro/v$(v)"
	@LANG=en_GB git push --tags


