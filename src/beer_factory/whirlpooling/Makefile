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

.PHONY: all demo test clean

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
