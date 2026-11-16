CC ?= cc
CPPFLAGS ?=
CFLAGS ?= -std=c11 -Wall -Wextra -Wpedantic -Werror -O2
LDFLAGS ?=
LDLIBS ?=

.PHONY: all demo test

all: build/tank-demo

build:
	mkdir -p $@

build/tank-demo: src/tank.c src/tank.h examples/demo.c | build
	$(CC) $(CPPFLAGS) -Isrc $(CFLAGS) src/tank.c examples/demo.c $(LDFLAGS) $(LDLIBS) -o $@

build/test-tank: src/tank.c src/tank.h tests/test_tank.c | build
	$(CC) $(CPPFLAGS) -Isrc $(CFLAGS) src/tank.c tests/test_tank.c $(LDFLAGS) $(LDLIBS) -o $@

demo: build/tank-demo
	./build/tank-demo

test: build/test-tank
	./build/test-tank
