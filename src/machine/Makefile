CC ?= cc
CFLAGS ?= -O2 -g
WARN = -std=c11 -Wall -Wextra -Werror -pedantic
CORE_LIBS = $(shell pkg-config --libs sqlite3 2>/dev/null || echo -lsqlite3) -lm
CORE_FLAGS = $(shell pkg-config --cflags sqlite3 2>/dev/null)
TRANSPORT_FLAGS = $(shell pkg-config --cflags libcjson rdkafka 2>/dev/null)
TRANSPORT_LIBS = $(shell pkg-config --libs libcjson rdkafka 2>/dev/null) -lpaho-mqtt3a -pthread
.PHONY: all check sanitize clean
all: build/beer-machine build/machinectl
build:
	mkdir -p build
build/beer-machine: machine.c machine.h main.c wire.c wire.h | build
	$(CC) $(CFLAGS) $(WARN) $(CORE_FLAGS) $(TRANSPORT_FLAGS) machine.c wire.c main.c -o $@ $(CORE_LIBS) $(TRANSPORT_LIBS)
build/machinectl: machinectl.c | build
	$(CC) $(CFLAGS) $(WARN) machinectl.c -o $@
build/test-core: machine.c machine.h tests/test_core.c | build
	$(CC) $(CFLAGS) $(WARN) $(CORE_FLAGS) -I. machine.c tests/test_core.c -o $@ $(CORE_LIBS)
check: build/test-core build/machinectl
	./build/test-core
sanitize:
	$(MAKE) clean
	$(MAKE) check CFLAGS='-O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer'
clean:
	rm -rf build
