# Use gcc unless CC is set explicitly (make's built-in default "cc" does not
# exist on MinGW/MSYS2).
ifeq ($(origin CC),default)
CC := gcc
endif
CPPFLAGS ?=
CFLAGS ?= -std=c11 -Wall -Wextra -Wpedantic -Werror -O2
LDFLAGS ?=
LDLIBS ?=

TIME_DIVISOR ?= 1
CPPFLAGS += -DTIME_DIVISOR=$(TIME_DIVISOR)

CORE := src/whirlpooling.c src/whirlpooling.h

# ---- Controller (Kafka + MQTT heartbeat) -----------------------------------
# Needs librdkafka and MQTT-C, so build it on Linux (WSL or a container).
KAFKA_COMMONS ?= ../machine_commons/machine_kafka
MQTT_COMMONS ?= ../machine_commons/machine_mqtt
MQTT_C_DIR ?=
MQTT_LIBS ?= -lmqttc

PKG_CONFIG ?= pkg-config
KAFKA_CFLAGS ?= $(shell $(PKG_CONFIG) --cflags rdkafka 2>/dev/null)
KAFKA_LIBS ?= $(shell $(PKG_CONFIG) --libs rdkafka 2>/dev/null || echo -lrdkafka)

# Use an installed MQTT-C library, or compile a checkout supplied by MQTT_C_DIR.
ifneq ($(strip $(MQTT_C_DIR)),)
MQTT_SOURCES = $(MQTT_C_DIR)/src/mqtt.c $(MQTT_C_DIR)/src/mqtt_pal.c
MQTT_CPPFLAGS = -I$(MQTT_C_DIR)/include
MQTT_LINK =
else
MQTT_SOURCES =
MQTT_LINK = $(MQTT_LIBS)
endif

# heartbeat.c uses getaddrinfo, which -std=c11 hides without this.
POSIX_CPPFLAGS := -D_POSIX_C_SOURCE=200809L

CONTROLLER_SOURCES := src/main.c src/whirlpooling.c \
	$(MQTT_COMMONS)/heartbeat.c $(KAFKA_COMMONS)/machine_kafka.c $(MQTT_SOURCES)
CONTROLLER_HEADERS := src/whirlpooling.h $(MQTT_COMMONS)/heartbeat.h \
	$(MQTT_COMMONS)/mqtt.h $(MQTT_COMMONS)/mqtt_pal.h \
	$(KAFKA_COMMONS)/machine_kafka.h

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

build/whirlpooling: $(CONTROLLER_SOURCES) $(CONTROLLER_HEADERS) | build
	$(CC) $(CPPFLAGS) $(POSIX_CPPFLAGS) -Isrc -I$(KAFKA_COMMONS) -I$(MQTT_COMMONS) $(MQTT_CPPFLAGS) $(KAFKA_CFLAGS) $(CFLAGS) $(CONTROLLER_SOURCES) $(LDFLAGS) $(MQTT_LINK) $(KAFKA_LIBS) $(LDLIBS) -pthread -o $@

controller: build/whirlpooling

# The controller, talking to Kafka at KAFKA_BROKERS. The MQTT heartbeat goes
# to localhost:1883 by default (MQTT_HOST / MQTT_PORT override it); without a broker it only
# prints a warning and keeps running.
run: build/whirlpooling
	KAFKA_BROKERS=$(KAFKA_BROKERS) ./build/whirlpooling $(MACHINE_ID)

clean:
	rm -rf build
