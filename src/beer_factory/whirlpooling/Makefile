# Use gcc unless CC is set explicitly (make's built-in default "cc" does not
# exist on MinGW/MSYS2).
ifeq ($(origin CC),default)
CC := gcc
endif
CPPFLAGS ?=
CFLAGS ?= -std=c11 -Wall -Wextra -Wpedantic -Werror -O2
LDFLAGS ?=
LDLIBS ?=

CORE := src/whirlpooling.c src/whirlpooling.h

.PHONY: all demo test clean

all: build/whirlpooling-demo

build:
	mkdir -p $@

build/whirlpooling-demo: $(CORE) examples/demo.c | build
	$(CC) $(CPPFLAGS) -Isrc $(CFLAGS) src/whirlpooling.c examples/demo.c $(LDFLAGS) $(LDLIBS) -o $@

build/test-whirlpooling: $(CORE) tests/test_whirlpooling.c | build
	$(CC) $(CPPFLAGS) -Isrc $(CFLAGS) src/whirlpooling.c tests/test_whirlpooling.c $(LDFLAGS) $(LDLIBS) -o $@

demo: build/whirlpooling-demo
	./build/whirlpooling-demo

test: build/test-whirlpooling
	./build/test-whirlpooling

clean:
	rm -rf build
