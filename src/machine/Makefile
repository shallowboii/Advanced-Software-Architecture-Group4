CC ?= cc
CFLAGS ?= -O2 -g
WARN = -std=c11 -Wall -Wextra -Werror -pedantic
CORE_LIBS = -lm
TRANSPORT_FLAGS = $(shell pkg-config --cflags libcjson rdkafka 2>/dev/null)
TRANSPORT_LIBS = $(shell pkg-config --libs libcjson rdkafka 2>/dev/null) -lpaho-mqtt3a -pthread
.PHONY: all check check-wire sanitize clean
all: build/beer-machine build/machinectl
build:
	mkdir -p build
build/beer-machine: machine.c machine.h main.c wire.c wire.h | build
	$(CC) $(CFLAGS) $(WARN) $(TRANSPORT_FLAGS) machine.c wire.c main.c -o $@ $(CORE_LIBS) $(TRANSPORT_LIBS)
build/machinectl: machinectl.c | build
	$(CC) $(CFLAGS) $(WARN) machinectl.c -o $@
build/test-core: machine.c machine.h tests/test_core.c | build
	$(CC) $(CFLAGS) $(WARN) -I. machine.c tests/test_core.c -o $@ $(CORE_LIBS)
check: build/test-core build/machinectl
	./build/test-core
build/test-wire: machine.c machine.h wire.c wire.h tests/test_wire.c | build
	$(CC) $(CFLAGS) $(WARN) $(TRANSPORT_FLAGS) -I. machine.c wire.c tests/test_wire.c -o $@ -lm $(shell pkg-config --libs libcjson)
check-wire: build/test-wire
	./build/test-wire
sanitize:
	$(MAKE) clean
	$(MAKE) check CFLAGS='-O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer'
clean:
	rm -rf build
