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
PYTHON ?= python3

TIME_DIVISOR ?= 1
CPPFLAGS += -DTIME_DIVISOR=$(TIME_DIVISOR)

.PHONY: all test test-controller mqtt test-mqtt clean

all: build/boiling-mqtt

build:
	mkdir -p $@

build/test-boiling: src/boiling.c src/boiling.h tests/test_boiling.c | build
	$(CC) $(CPPFLAGS) -Isrc $(CFLAGS) src/boiling.c tests/test_boiling.c $(LDFLAGS) $(LDLIBS) -o $@

test: build/test-boiling test-controller
	./build/test-boiling

# Simulate heartbeat I/O and elapsed time without a broker or MQTT-C library.
build/test-controller: src/main.c src/boiling.c src/boiling.h tests/controller_stubs.c tests/include/librdkafka/rdkafka.h $(KAFKA_COMMONS)/machine_kafka.h $(MQTT_COMMONS)/heartbeat.h $(MQTT_COMMONS)/mqtt.h $(MQTT_COMMONS)/mqtt_pal.h | build
	$(CC) $(CPPFLAGS) -D_POSIX_C_SOURCE=200809L -Isrc -Itests/include -I$(KAFKA_COMMONS) -I$(MQTT_COMMONS) $(CFLAGS) -Wno-strict-prototypes -Dclock_gettime=test_clock_gettime -Dnanosleep=test_nanosleep src/main.c src/boiling.c tests/controller_stubs.c $(LDFLAGS) $(LDLIBS) -o $@

test-controller: build/test-controller
	$(PYTHON) tests/test_controller.py ./build/test-controller

# Use an installed MQTT-C library, or compile a checkout supplied by MQTT_C_DIR.
ifneq ($(strip $(MQTT_C_DIR)),)
MQTT_SOURCES = $(MQTT_C_DIR)/src/mqtt.c $(MQTT_C_DIR)/src/mqtt_pal.c
MQTT_CPPFLAGS = -I$(MQTT_C_DIR)/include
MQTT_LINK =
else
MQTT_SOURCES =
MQTT_LINK = $(MQTT_LIBS)
endif

build/boiling-mqtt: src/main.c src/boiling.c src/boiling.h $(MQTT_COMMONS)/heartbeat.c $(MQTT_COMMONS)/heartbeat.h $(MQTT_COMMONS)/mqtt.h $(MQTT_COMMONS)/mqtt_pal.h $(MQTT_SOURCES) $(KAFKA_COMMONS)/machine_kafka.c $(KAFKA_COMMONS)/machine_kafka.h | build
	$(CC) $(CPPFLAGS) -D_POSIX_C_SOURCE=200809L -I$(KAFKA_COMMONS) $(KAFKA_CFLAGS) -Isrc -I$(MQTT_COMMONS) $(MQTT_CPPFLAGS) $(CFLAGS) src/main.c src/boiling.c $(MQTT_COMMONS)/heartbeat.c $(MQTT_SOURCES) $(KAFKA_COMMONS)/machine_kafka.c $(LDFLAGS) $(KAFKA_LIBS) $(MQTT_LINK) $(LDLIBS) -pthread -o $@

mqtt: build/boiling-mqtt

test-mqtt: build/boiling-mqtt
	$(PYTHON) tests/test_mqtt.py ./build/boiling-mqtt

clean:
	$(RM) -r build
