CC ?= cc
CPPFLAGS ?=
CFLAGS ?= -std=c11 -Wall -Wextra -Wpedantic -Werror -O2
LDFLAGS ?=
LDLIBS ?=
MQTT_COMMONS ?= ../machine_commons/machine_mqtt
MQTT_C_DIR ?=
MQTT_LIBS ?= -lmqttc
PYTHON ?= python3

.PHONY: all test mqtt test-mqtt clean

all: build/boiling-mqtt

build:
	mkdir -p $@

build/test-boiling: src/boiling.c src/boiling.h tests/test_boiling.c | build
	$(CC) $(CPPFLAGS) -Isrc $(CFLAGS) src/boiling.c tests/test_boiling.c $(LDFLAGS) $(LDLIBS) -o $@

test: build/test-boiling
	./build/test-boiling

# Use an installed MQTT-C library, or compile a checkout supplied by MQTT_C_DIR.
ifneq ($(strip $(MQTT_C_DIR)),)
MQTT_SOURCES = $(MQTT_C_DIR)/src/mqtt.c $(MQTT_C_DIR)/src/mqtt_pal.c
MQTT_CPPFLAGS = -I$(MQTT_C_DIR)/include
MQTT_LINK =
else
MQTT_SOURCES =
MQTT_LINK = $(MQTT_LIBS)
endif

build/boiling-mqtt: src/main.c src/boiling.c src/boiling.h $(MQTT_COMMONS)/heartbeat.c $(MQTT_COMMONS)/heartbeat.h $(MQTT_COMMONS)/mqtt.h $(MQTT_COMMONS)/mqtt_pal.h $(MQTT_SOURCES) | build
	$(CC) $(CPPFLAGS) -Isrc -I$(MQTT_COMMONS) $(MQTT_CPPFLAGS) $(CFLAGS) src/main.c src/boiling.c $(MQTT_COMMONS)/heartbeat.c $(MQTT_SOURCES) $(LDFLAGS) $(MQTT_LINK) $(LDLIBS) -pthread -o $@

mqtt: build/boiling-mqtt

test-mqtt: build/boiling-mqtt
	$(PYTHON) tests/test_mqtt.py ./build/boiling-mqtt

clean:
	$(RM) -r build
