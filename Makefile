.PHONY: build test

build: test

test:
	deno test --allow-all --unstable --failfast tests/

