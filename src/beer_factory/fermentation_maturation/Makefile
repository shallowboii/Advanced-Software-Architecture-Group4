CC ?= cc
CPPFLAGS ?=
CFLAGS ?= -std=c11 -Wall -Wextra -Wpedantic -Werror -O2
LDFLAGS ?=
LDLIBS ?=
KAFKA_COMMONS ?= ../machine_commons/machine_kafka
PKG_CONFIG ?= pkg-config
KAFKA_CFLAGS ?= $(shell $(PKG_CONFIG) --cflags rdkafka 2>/dev/null)
KAFKA_LIBS ?= $(shell $(PKG_CONFIG) --libs rdkafka 2>/dev/null || echo -lrdkafka)
MQTT_COMMONS ?= ../machine_commons/machine_mqtt
MQTT_C_DIR ?=
MQTT_LIBS ?= -lmqttc

.PHONY: all test test-reuse mqtt clean

all: build/tank-mqtt

build:
	mkdir -p $@

build/test-tank: src/tank.c src/tank.h tests/test_tank.c | build
	$(CC) $(CPPFLAGS) -Isrc $(CFLAGS) src/tank.c tests/test_tank.c $(LDFLAGS) $(LDLIBS) -o $@

test: build/test-tank
	./build/test-tank

build/test-tank-reuse: src/tank.c src/tank.h tests/test_tank_reuse.c | build
	$(CC) $(CPPFLAGS) -Isrc $(CFLAGS) -UNDEBUG src/tank.c tests/test_tank_reuse.c $(LDFLAGS) $(LDLIBS) -o $@

test-reuse: build/test-tank-reuse
	./build/test-tank-reuse

# Use an installed MQTT-C library, or compile a checkout supplied by MQTT_C_DIR.
ifneq ($(strip $(MQTT_C_DIR)),)
MQTT_SOURCES = $(MQTT_C_DIR)/src/mqtt.c $(MQTT_C_DIR)/src/mqtt_pal.c
MQTT_CPPFLAGS = -I$(MQTT_C_DIR)/include
MQTT_LINK =
else
MQTT_SOURCES =
MQTT_LINK = $(MQTT_LIBS)
endif

build/tank-mqtt: src/main.c src/tank.c src/tank.h $(MQTT_COMMONS)/heartbeat.c $(MQTT_COMMONS)/heartbeat.h $(MQTT_COMMONS)/mqtt.h $(MQTT_COMMONS)/mqtt_pal.h $(MQTT_SOURCES) $(KAFKA_COMMONS)/machine_kafka.c $(KAFKA_COMMONS)/machine_kafka.h | build
	$(CC) $(CPPFLAGS) -D_POSIX_C_SOURCE=200809L -I$(KAFKA_COMMONS) $(KAFKA_CFLAGS) -Isrc -I$(MQTT_COMMONS) $(MQTT_CPPFLAGS) $(CFLAGS) src/main.c src/tank.c $(MQTT_COMMONS)/heartbeat.c $(MQTT_SOURCES) $(KAFKA_COMMONS)/machine_kafka.c $(LDFLAGS) $(KAFKA_LIBS) $(MQTT_LINK) $(LDLIBS) -pthread -o $@

mqtt: build/tank-mqtt

clean:
	$(RM) -r build
