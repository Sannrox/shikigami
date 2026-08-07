CARGO ?= cargo

.PHONY: all build update validate test test-integration embed

all: build

build:
	$(CARGO) build --locked --all-targets

update:
	$(CARGO) fmt --all

validate:
	python3 scripts/validate-docs.py
	$(CARGO) fmt --all -- --check
	$(CARGO) clippy --all-targets --locked -- -D warnings

test:
	$(CARGO) test --locked --lib

test-integration:
	$(CARGO) test --locked --tests

embed:
	$(CARGO) run --locked --example embed_smoke
