CC ?= cc
CPPFLAGS ?=
CFLAGS ?= -std=c11 -Wall -Wextra -Wpedantic -Werror -O2
LDFLAGS ?=
LDLIBS ?=
MQTT_COMMONS ?= ../machine_commons/machine_mqtt
MQTT_C_DIR ?=
MQTT_LIBS ?= -lmqttc

.PHONY: all test clean

all: build/mashing

build:
	mkdir -p $@

build/test-mashing: src/mashing.c src/mashing.h tests/test_mashing.c | build
	$(CC) $(CPPFLAGS) -Isrc $(CFLAGS) src/mashing.c tests/test_mashing.c $(LDFLAGS) $(LDLIBS) -o $@

test: build/test-mashing
	./build/test-mashing

# Use an installed MQTT-C library, or compile a checkout supplied by MQTT_C_DIR.
ifneq ($(strip $(MQTT_C_DIR)),)
MQTT_SOURCES = $(MQTT_C_DIR)/src/mqtt.c $(MQTT_C_DIR)/src/mqtt_pal.c
MQTT_CPPFLAGS = -I$(MQTT_C_DIR)/include
MQTT_LINK =
else
MQTT_SOURCES =
MQTT_LINK = $(MQTT_LIBS)
endif

build/mashing: CPPFLAGS += -D_POSIX_C_SOURCE=200809L
build/mashing: src/main.c src/mashing.c src/mashing.h $(MQTT_COMMONS)/heartbeat.c $(MQTT_COMMONS)/heartbeat.h $(MQTT_COMMONS)/mqtt.h $(MQTT_COMMONS)/mqtt_pal.h $(MQTT_SOURCES) | build
	$(CC) $(CPPFLAGS) -Isrc -I$(MQTT_COMMONS) $(MQTT_CPPFLAGS) $(CFLAGS) src/main.c src/mashing.c $(MQTT_COMMONS)/heartbeat.c $(MQTT_SOURCES) $(LDFLAGS) $(MQTT_LINK) $(LDLIBS) -pthread -o $@


clean:
	$(RM) -r build
