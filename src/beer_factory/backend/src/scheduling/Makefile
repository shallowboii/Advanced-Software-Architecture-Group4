# Kafka scheduler service (src/scheduler*.c, src/machine_table.c,
# src/dispatch.c). The offline greedy planner in src/ is built separately.
# On Windows run these targets through the dev container:
#   ../../../machine_commons/dev/run.sh make test
ifeq ($(origin CC),default)
CC := gcc
endif
CPPFLAGS ?=
CFLAGS ?= -std=c11 -Wall -Wextra -Wpedantic -Werror -O2
LDFLAGS ?=
LDLIBS ?=

KAFKA_COMMONS ?= ../../../machine_commons/machine_kafka
PKG_CONFIG ?= pkg-config
KAFKA_CFLAGS ?= $(shell $(PKG_CONFIG) --cflags rdkafka libcjson 2>/dev/null)
KAFKA_LIBS ?= $(shell $(PKG_CONFIG) --libs rdkafka libcjson 2>/dev/null || echo -lrdkafka -lcjson)
CJSON_LIBS ?= $(shell $(PKG_CONFIG) --libs libcjson 2>/dev/null || echo -lcjson)

INCLUDES := -Isrc -I$(KAFKA_COMMONS) $(KAFKA_CFLAGS)
KAFKA_SOURCES := $(addprefix $(KAFKA_COMMONS)/,message.c bus.c)
SOURCES := src/scheduler_main.c src/scheduler.c src/machine_table.c src/dispatch.c
HEADERS := src/scheduler.h $(KAFKA_COMMONS)/machine_kafka.h

KAFKA_BROKERS ?= localhost:9092

.PHONY: all scheduler test run clean

all: scheduler

build:
	mkdir -p $@

build/scheduler: $(SOURCES) $(HEADERS) $(KAFKA_SOURCES) | build
	$(CC) $(CPPFLAGS) $(INCLUDES) $(CFLAGS) $(SOURCES) $(KAFKA_SOURCES) $(LDFLAGS) $(KAFKA_LIBS) -lm $(LDLIBS) -o $@

scheduler: build/scheduler

# Machine table only: no broker, only cJSON.
build/test-scheduler: tests/test_scheduler.c src/machine_table.c $(HEADERS) $(KAFKA_COMMONS)/message.c | build
	$(CC) $(CPPFLAGS) $(INCLUDES) $(CFLAGS) tests/test_scheduler.c src/machine_table.c $(KAFKA_COMMONS)/message.c $(LDFLAGS) $(CJSON_LIBS) -lm $(LDLIBS) -o $@

test: build/test-scheduler
	./build/test-scheduler

run: build/scheduler
	KAFKA_BROKERS=$(KAFKA_BROKERS) ./build/scheduler

clean:
	rm -rf build
