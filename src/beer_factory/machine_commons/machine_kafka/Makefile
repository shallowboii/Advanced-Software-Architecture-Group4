# Use gcc unless CC is set explicitly (make's built-in default "cc" does not
# exist on MinGW/MSYS2).
ifeq ($(origin CC),default)
CC := gcc
endif
AR ?= ar
CPPFLAGS ?=
CFLAGS ?= -std=c11 -Wall -Wextra -Wpedantic -Werror -O2

# librdkafka from the system
# (Arch: pacman -S librdkafka, Debian: apt-get install librdkafka-dev).
PKG_CONFIG ?= pkg-config
KAFKA_CFLAGS ?= $(shell $(PKG_CONFIG) --cflags rdkafka 2>/dev/null)
KAFKA_LIBS ?= $(shell $(PKG_CONFIG) --libs rdkafka 2>/dev/null || echo -lrdkafka)

.PHONY: all test libs clean

all: build/libmachinekafka.a

build:
	mkdir -p $@

build/machine_kafka.o: machine_kafka.c machine_kafka.h | build
	$(CC) $(CPPFLAGS) $(KAFKA_CFLAGS) $(CFLAGS) -c $< -o $@

build/libmachinekafka.a: build/machine_kafka.o
	$(AR) rcs $@ $^

build/test-machine-kafka: tests/test_machine_kafka.c machine_kafka.c machine_kafka.h | build
	$(CC) $(CPPFLAGS) $(KAFKA_CFLAGS) -I. $(CFLAGS) -UNDEBUG tests/test_machine_kafka.c machine_kafka.c $(LDFLAGS) $(KAFKA_LIBS) $(LDLIBS) -o $@

test: build/test-machine-kafka
	./build/test-machine-kafka

# Prints the flags a machine needs to link against this library.
libs:
	@echo "-I$(CURDIR) $(KAFKA_CFLAGS) $(CURDIR)/build/libmachinekafka.a $(KAFKA_LIBS)"

clean:
	rm -rf build
