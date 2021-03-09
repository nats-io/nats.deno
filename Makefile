.PHONY: build test

build: test

test:
	rm -rf ./coverage
	deno test --allow-all --unstable --reload --coverage=coverage --failfast tests/

cover:
	deno coverage --unstable ./coverage --lcov > ./coverage/out.lcov
	genhtml -o ./coverage/html ./coverage/out.lcov
	open ./coverage/html/index.html

