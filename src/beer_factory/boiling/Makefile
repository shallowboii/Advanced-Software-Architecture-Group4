CC ?= cc
CPPFLAGS ?=
CFLAGS ?= -std=c11 -Wall -Wextra -Wpedantic -Werror -O2
LDFLAGS ?=
LDLIBS ?=

.PHONY: all demo test

all: build/boiling-demo

build:
	mkdir -p $@

build/boiling-demo: src/boiling.c src/boiling.h examples/demo.c | build
	$(CC) $(CPPFLAGS) -Isrc $(CFLAGS) src/boiling.c examples/demo.c $(LDFLAGS) $(LDLIBS) -o $@

build/test-boiling: src/boiling.c src/boiling.h tests/test_boiling.c | build
	$(CC) $(CPPFLAGS) -Isrc $(CFLAGS) src/boiling.c tests/test_boiling.c $(LDFLAGS) $(LDLIBS) -o $@

demo: build/boiling-demo
	./build/boiling-demo

test: build/test-boiling
	./build/test-boiling
