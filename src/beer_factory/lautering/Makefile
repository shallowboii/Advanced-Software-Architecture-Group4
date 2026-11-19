# Use gcc unless CC is set explicitly (make's built-in default "cc" does not
# exist on MinGW/MSYS2).
ifeq ($(origin CC),default)
CC := gcc
endif
CPPFLAGS ?=
CFLAGS ?= -std=c11 -Wall -Wextra -Wpedantic -Werror -O2
LDFLAGS ?=
LDLIBS ?=
MQTT_COMMONS ?= ../machine_commons/machine_mqtt
MQTT_C_DIR ?=
MQTT_LIBS ?= -lmqttc

CORE := src/lautering.c src/lautering.h

.PHONY: all test clean

all: build/lautering

build:
	mkdir -p $@

build/test-lautering: $(CORE) tests/test_lautering.c | build
	$(CC) $(CPPFLAGS) -Isrc $(CFLAGS) src/lautering.c tests/test_lautering.c $(LDFLAGS) $(LDLIBS) -o $@

test: build/test-lautering
	./build/test-lautering

# Use an installed MQTT-C library, or compile a checkout supplied by MQTT_C_DIR.
ifneq ($(strip $(MQTT_C_DIR)),)
MQTT_SOURCES = $(MQTT_C_DIR)/src/mqtt.c $(MQTT_C_DIR)/src/mqtt_pal.c
MQTT_CPPFLAGS = -I$(MQTT_C_DIR)/include
MQTT_LINK =
else
MQTT_SOURCES =
MQTT_LINK = $(MQTT_LIBS)
endif

build/lautering: src/main.c $(CORE) $(MQTT_COMMONS)/heartbeat.c $(MQTT_COMMONS)/heartbeat.h $(MQTT_COMMONS)/mqtt.h $(MQTT_COMMONS)/mqtt_pal.h $(MQTT_SOURCES) | build
	$(CC) $(CPPFLAGS) -Isrc -I$(MQTT_COMMONS) $(MQTT_CPPFLAGS) $(CFLAGS) src/main.c src/lautering.c $(MQTT_COMMONS)/heartbeat.c $(MQTT_SOURCES) $(LDFLAGS) $(MQTT_LINK) $(LDLIBS) -pthread -o $@

clean:
	$(RM) -r build
