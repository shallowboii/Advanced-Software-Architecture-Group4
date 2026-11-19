# Use gcc unless CC is set explicitly (make's built-in default "cc" does not
# exist on MinGW/MSYS2).
ifeq ($(origin CC),default)
CC := gcc
endif
AR ?= ar
CPPFLAGS ?=
CFLAGS ?= -std=c11 -Wall -Wextra -Wpedantic -Werror -O2
LDFLAGS ?=
LDLIBS ?=

# librdkafka and cJSON from the system (Arch: pacman -S librdkafka cjson,
# Debian: apt-get install librdkafka-dev libcjson-dev).
PKG_CONFIG ?= pkg-config
KAFKA_CFLAGS ?= $(shell $(PKG_CONFIG) --cflags rdkafka libcjson 2>/dev/null)
KAFKA_LIBS ?= $(shell $(PKG_CONFIG) --libs rdkafka libcjson 2>/dev/null || echo -lrdkafka -lcjson)
CJSON_LIBS ?= $(shell $(PKG_CONFIG) --libs libcjson 2>/dev/null || echo -lcjson)

KAFKA_BROKERS ?= localhost:9092

.PHONY: all test test-kafka libs clean

all: build/libmachinekafka.a

build:
	mkdir -p $@

build/mk_message.o: mk_message.c machine_kafka.h | build
	$(CC) $(CPPFLAGS) $(KAFKA_CFLAGS) $(CFLAGS) -c mk_message.c -o $@

build/mk_bus.o: mk_bus.c machine_kafka.h | build
	$(CC) $(CPPFLAGS) $(KAFKA_CFLAGS) $(CFLAGS) -c mk_bus.c -o $@

build/libmachinekafka.a: build/mk_message.o build/mk_bus.o
	$(AR) rcs $@ $^

# Message tests: no broker, only cJSON.
build/test-message: mk_message.c machine_kafka.h tests/test_message.c | build
	$(CC) $(CPPFLAGS) -I. $(KAFKA_CFLAGS) $(CFLAGS) mk_message.c tests/test_message.c $(LDFLAGS) $(CJSON_LIBS) -lm $(LDLIBS) -o $@

# Bus test: needs a running broker at KAFKA_BROKERS.
build/test-bus: build/libmachinekafka.a tests/test_bus.c | build
	$(CC) $(CPPFLAGS) -I. $(KAFKA_CFLAGS) $(CFLAGS) tests/test_bus.c build/libmachinekafka.a $(LDFLAGS) $(KAFKA_LIBS) -lm $(LDLIBS) -o $@

test: build/test-message
	./build/test-message

test-kafka: build/test-bus
	KAFKA_BROKERS=$(KAFKA_BROKERS) ./build/test-bus

# Prints the flags a machine needs to link against this library.
libs:
	@echo "-I$(CURDIR) $(KAFKA_CFLAGS) $(CURDIR)/build/libmachinekafka.a $(KAFKA_LIBS) -lm"

clean:
	rm -rf build
