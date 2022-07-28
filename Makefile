.PHONY: build test bundle lint

build: test

lint:
	deno lint --unstable

test: clean
	deno test --allow-all --unstable --reload --quiet --parallel --coverage=coverage --fail-fast tests/

testw: clean
	deno test --allow-all --unstable --reload --parallel --watch --fail-fast tests/

cover:
	deno coverage --unstable ./coverage --lcov > ./coverage/out.lcov
	genhtml -o ./coverage/html ./coverage/out.lcov
	open ./coverage/html/index.html

clean:
	rm -rf ./coverage

bundle:
	deno bundle --log-level info --unstable src/mod.ts ./nats.js

fmt:
	deno fmt src/ doc/ bin/ nats-base-client/ examples/ tests/ jetstream.md README.md
