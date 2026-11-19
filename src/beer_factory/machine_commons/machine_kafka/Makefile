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

OBJS := build/message.o build/ledger.o build/bus.o build/trace.o

.PHONY: all test test-kafka trace libs clean

all: build/libmachinekafka.a

build:
	mkdir -p $@

build/%.o: %.c machine_kafka.h trace.h | build
	$(CC) $(CPPFLAGS) $(KAFKA_CFLAGS) $(CFLAGS) -c $< -o $@

build/libmachinekafka.a: $(OBJS)
	$(AR) rcs $@ $^

# Message and ledger tests: no broker, only cJSON.
build/test-message: message.c trace.c machine_kafka.h trace.h tests/test_message.c | build
	$(CC) $(CPPFLAGS) -I. $(KAFKA_CFLAGS) $(CFLAGS) message.c trace.c tests/test_message.c $(LDFLAGS) $(CJSON_LIBS) -lm $(LDLIBS) -o $@

build/test-ledger: message.c ledger.c trace.c machine_kafka.h trace.h tests/test_ledger.c | build
	$(CC) $(CPPFLAGS) -I. $(KAFKA_CFLAGS) $(CFLAGS) message.c ledger.c trace.c tests/test_ledger.c $(LDFLAGS) $(CJSON_LIBS) -lm $(LDLIBS) -o $@

# Bus test: needs a running broker at KAFKA_BROKERS (see docker-compose.yml).
build/test-bus: build/libmachinekafka.a tests/test_bus.c | build
	$(CC) $(CPPFLAGS) -I. $(KAFKA_CFLAGS) $(CFLAGS) tests/test_bus.c build/libmachinekafka.a $(LDFLAGS) $(KAFKA_LIBS) -lm $(LDLIBS) -o $@

test: build/test-message build/test-ledger
	./build/test-message
	./build/test-ledger

test-kafka: build/test-bus
	KAFKA_BROKERS=$(KAFKA_BROKERS) ./build/test-bus

# Bus test with the library trace on; every call and Kafka event ends up in
# build/trace.log (paste it into the trace viewer of the debug page).
trace: build/test-bus
	MK_TRACE=1 KAFKA_BROKERS=$(KAFKA_BROKERS) ./build/test-bus 2> build/trace.log
	@grep -c '^mk ' build/trace.log | xargs echo "trace lines in build/trace.log:"

# Prints the flags a machine needs to link against this library.
libs:
	@echo "-I$(CURDIR) $(KAFKA_CFLAGS) $(CURDIR)/build/libmachinekafka.a $(KAFKA_LIBS) -lm"

clean:
	rm -rf build
