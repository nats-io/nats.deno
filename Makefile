.PHONY: build test bundle lint

build: test

lint:
	deno lint --ignore=docs/,debug/

test: clean
	deno test --allow-all --parallel --reload --quiet --coverage=coverage tests/ jetstream/tests kv/tests/ os/tests/ service/tests/
	deno test --allow-all --parallel --reload --quiet --unsafely-ignore-certificate-errors --coverage=coverage unsafe_tests/


testw: clean
	deno test --allow-all --unstable --reload --parallel --watch --fail-fast tests/ jetstream/ kv/tests/ os/tests/ service/tests/

cover:
	deno coverage --unstable ./coverage --lcov > ./coverage/out.lcov
	genhtml -o ./coverage/html ./coverage/out.lcov
	open ./coverage/html/index.html

clean:
	rm -rf ./coverage

bundle:
	deno bundle --log-level info --unstable src/mod.ts ./nats.js

fmt:
	deno fmt src/ doc/ bin/ nats-base-client/ examples/ tests/ debug/ unsafe_tests/ jetstream/ kv/ os/ service/ jetstream.md README.md migration.md services.md
