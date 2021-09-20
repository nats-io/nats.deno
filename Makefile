.PHONY: build test bundle

build: test

test: clean
	deno lint --unstable
	deno test --allow-all --unstable --reload --coverage=coverage --fail-fast tests/

cover:
	deno coverage --unstable ./coverage --lcov > ./coverage/out.lcov
	genhtml -o ./coverage/html ./coverage/out.lcov
	open ./coverage/html/index.html

clean:
	rm -rf ./coverage

bundle:
	deno bundle --log-level info --unstable src/mod.ts ./nats.js

fmt:
	deno fmt src/ doc/ bin/ nats-base-client/ examples/ tests/
