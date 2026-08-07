# Make targets are thin wrappers around deterministic repository scripts.
.EXPORT_ALL_VARIABLES:

CARGO ?= cargo
WHAT ?=

.PHONY: all build validate update test test-integration test-e2e embed

all:
	./scripts/make-targets/build.sh $(WHAT)

build: all

validate:
	./scripts/make-targets/validate.sh

update:
	./scripts/make-targets/update.sh

test:
	./scripts/make-targets/test.sh $(WHAT)

test-integration:
	./scripts/make-targets/test-integration.sh $(WHAT)

test-e2e:
	./scripts/make-targets/test-e2e.sh

embed: test-e2e
