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

# librdkafka (>= 2.0) and cJSON (>= 1.7.13) from the system
# (Arch: pacman -S librdkafka cjson,
#  Debian: apt-get install librdkafka-dev libcjson-dev).
PKG_CONFIG ?= pkg-config
KAFKA_CFLAGS ?= $(shell $(PKG_CONFIG) --cflags rdkafka libcjson 2>/dev/null)
KAFKA_LIBS ?= $(shell $(PKG_CONFIG) --libs rdkafka libcjson 2>/dev/null || echo -lrdkafka -lcjson)
CJSON_LIBS ?= $(shell $(PKG_CONFIG) --libs libcjson 2>/dev/null || echo -lcjson)

KAFKA_BROKERS ?= localhost:9092

OBJS := build/message.o build/ledger.o build/bus.o

.PHONY: all test test-kafka libs clean

all: build/libmachinekafka.a

build:
	mkdir -p $@

build/%.o: %.c machine_kafka.h | build
	$(CC) $(CPPFLAGS) $(KAFKA_CFLAGS) $(CFLAGS) -c $< -o $@

build/libmachinekafka.a: $(OBJS)
	$(AR) rcs $@ $^

# Message and ledger tests: no broker, only cJSON.
build/test-message: message.c machine_kafka.h tests/test_message.c | build
	$(CC) $(CPPFLAGS) -I. $(KAFKA_CFLAGS) $(CFLAGS) message.c tests/test_message.c $(LDFLAGS) $(CJSON_LIBS) -lm $(LDLIBS) -o $@

build/test-ledger: message.c ledger.c machine_kafka.h tests/test_ledger.c | build
	$(CC) $(CPPFLAGS) -I. $(KAFKA_CFLAGS) $(CFLAGS) message.c ledger.c tests/test_ledger.c $(LDFLAGS) $(CJSON_LIBS) -lm $(LDLIBS) -o $@

# Bus test: needs a running broker at KAFKA_BROKERS (see docker-compose.yml).
build/test-bus: build/libmachinekafka.a tests/test_bus.c | build
	$(CC) $(CPPFLAGS) -I. $(KAFKA_CFLAGS) $(CFLAGS) tests/test_bus.c build/libmachinekafka.a $(LDFLAGS) $(KAFKA_LIBS) -lm $(LDLIBS) -o $@

test: build/test-message build/test-ledger
	./build/test-message
	./build/test-ledger

test-kafka: build/test-bus
	KAFKA_BROKERS=$(KAFKA_BROKERS) ./build/test-bus

# Prints the flags a machine needs to link against this library.
libs:
	@echo "-I$(CURDIR) $(KAFKA_CFLAGS) $(CURDIR)/build/libmachinekafka.a $(KAFKA_LIBS) -lm"

clean:
	rm -rf build
