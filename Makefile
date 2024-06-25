.PHONY: build test bundle lint

build: test

lint:
	deno lint --ignore=docs/,debug/

test: clean
	deno test --allow-all --parallel --reload --quiet --coverage=coverage core/tests/ jetstream/tests kv/tests/ obj/tests/ services/tests/
	deno test --allow-all --parallel --reload --quiet --unsafely-ignore-certificate-errors --coverage=coverage core/unsafe_tests/


testw: clean
	deno test --allow-all --unstable --reload --parallel --watch --fail-fast tests/ jetstream/ kv/tests/ obj/tests/ services/tests/

cover:
	deno coverage --unstable ./coverage --lcov > ./coverage/out.lcov
	genhtml -o ./coverage/html ./coverage/out.lcov
	open ./coverage/html/index.html

clean:
	rm -rf ./coverage

bundle:
	deno bundle --log-level info --unstable src/mod.ts ./nats.js
	deno bundle --log-level info --unstable jetstream/mod.ts ./jetstream.js
	deno bundle --log-level info --unstable kv/mod.ts ./kv.js
	deno bundle --log-level info --unstable obj/mod.ts ./os.js
	deno bundle --log-level info --unstable service/mod.ts ./svc.js


fmt:
	deno fmt transport-deno/ doc/ bin/ core/ examples/ tests/ debug/ unsafe_tests/ jetstream/ kv/ obj/ services/ test_helpers/ jetstream.md README.md migration.md services.md
