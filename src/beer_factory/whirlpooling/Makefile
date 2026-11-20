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

# ---- Controller (Kafka, optional MQTT heartbeat) ---------------------------
# Needs librdkafka and MQTT-C. On Windows run these targets in Linux (WSL or
# a container).
KAFKA_COMMONS ?= ../machine_commons/machine_kafka
MQTT_COMMONS ?= ../machine_commons/machine_mqtt
# MQTT-C checkout (src/mqtt.c, src/mqtt_pal.c); the dev image sets it.
# Empty: link an installed -lmqttc instead.
MQTT_C_DIR ?=
MQTT_LIBS ?= -lmqttc

PKG_CONFIG ?= pkg-config
KAFKA_CFLAGS ?= $(shell $(PKG_CONFIG) --cflags rdkafka 2>/dev/null)
KAFKA_LIBS ?= $(shell $(PKG_CONFIG) --libs rdkafka 2>/dev/null || echo -lrdkafka)

KAFKA_SOURCES := $(KAFKA_COMMONS)/machine_kafka.c
KAFKA_HEADERS := $(KAFKA_COMMONS)/machine_kafka.h
INCLUDES := -Isrc -I$(KAFKA_COMMONS) -I$(MQTT_COMMONS) $(KAFKA_CFLAGS)

ifneq ($(strip $(MQTT_C_DIR)),)
MQTT_OBJS := build/mqtt.o build/mqtt_pal.o
MQTT_LINK :=
else
MQTT_OBJS :=
MQTT_LINK := $(MQTT_LIBS)
endif

KAFKA_BROKERS ?= localhost:9092
MACHINE_ID ?= 1

.PHONY: all demo test controller run clean

all: build/whirlpooling

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

# Third-party MQTT-C, compiled with its own (relaxed) flags. Its <mqtt.h> is
# the copy in machine_mqtt/ (same version, v1.1.7).
build/mqtt.o: $(MQTT_C_DIR)/src/mqtt.c | build
	$(CC) -std=gnu11 -O2 -w -I$(MQTT_COMMONS) -c $< -o $@
build/mqtt_pal.o: $(MQTT_C_DIR)/src/mqtt_pal.c | build
	$(CC) -std=gnu11 -O2 -w -I$(MQTT_COMMONS) -c $< -o $@

build/whirlpooling: src/main.c $(CORE) $(MQTT_COMMONS)/heartbeat.c $(MQTT_COMMONS)/heartbeat.h $(KAFKA_SOURCES) $(KAFKA_HEADERS) $(MQTT_OBJS) | build
	$(CC) $(CPPFLAGS) $(INCLUDES) $(CFLAGS) src/main.c src/whirlpooling.c $(MQTT_COMMONS)/heartbeat.c $(KAFKA_SOURCES) $(MQTT_OBJS) $(LDFLAGS) $(MQTT_LINK) $(KAFKA_LIBS) $(LDLIBS) -pthread -o $@

controller: build/whirlpooling

# The controller. MQTT_HOST (and MQTT_PORT) from the environment turn the
# heartbeat on; without them it runs Kafka-only.
run: build/whirlpooling
	KAFKA_BROKERS=$(KAFKA_BROKERS) ./build/whirlpooling whirlpooling-$(MACHINE_ID)

clean:
	rm -rf build

MQTT_COMMONS ?= ../machine_commons/machine_mqtt
MQTT_C_DIR ?=
MQTT_LIBS ?= -lmqttc

# Use an installed MQTT-C library, or compile a checkout supplied by MQTT_C_DIR.
ifneq ($(strip $(MQTT_C_DIR)),)
MQTT_SOURCES = $(MQTT_C_DIR)/src/mqtt.c $(MQTT_C_DIR)/src/mqtt_pal.c
MQTT_CPPFLAGS = -I$(MQTT_C_DIR)/include
MQTT_LINK =
else
MQTT_SOURCES =
MQTT_LINK = $(MQTT_LIBS)
endif

build/whirlpooling: src/main.c $(MQTT_COMMONS)/heartbeat.c $(MQTT_COMMONS)/heartbeat.h $(MQTT_COMMONS)/mqtt.h $(MQTT_COMMONS)/mqtt_pal.h $(MQTT_SOURCES) | build
	$(CC) $(CPPFLAGS) -Isrc -I$(MQTT_COMMONS) $(MQTT_CPPFLAGS) $(CFLAGS) src/main.c $(MQTT_COMMONS)/heartbeat.c $(MQTT_SOURCES) $(LDFLAGS) $(MQTT_LINK) $(LDLIBS) -pthread -o $@
