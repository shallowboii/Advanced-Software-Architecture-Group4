# Use gcc unless CC is set explicitly (make's built-in default "cc" does not
# exist on MinGW/MSYS2).
ifeq ($(origin CC),default)
CC := gcc
endif
CPPFLAGS ?=
CFLAGS ?= -std=c11 -Wall -Wextra -Wpedantic -Werror -O2
LDFLAGS ?=
LDLIBS ?=
PYTHON ?= python3

CORE := src/whirlpooling.c src/whirlpooling.h

# ---- Controller (Kafka + MQTT heartbeat) ------------------------------------
# Needs librdkafka, cJSON and MQTT-C. On Windows run these targets through the
# dev container: ../machine_commons/dev/run.sh make test-kafka
KAFKA_COMMONS ?= ../machine_commons/machine_kafka
MQTT_COMMONS ?= ../machine_commons/machine_mqtt
# MQTT-C checkout (src/mqtt.c, src/mqtt_pal.c); the dev image sets it.
# Empty: link an installed -lmqttc instead.
MQTT_C_DIR ?=
MQTT_LIBS ?= -lmqttc

PKG_CONFIG ?= pkg-config
KAFKA_CFLAGS ?= $(shell $(PKG_CONFIG) --cflags rdkafka libcjson 2>/dev/null)
KAFKA_LIBS ?= $(shell $(PKG_CONFIG) --libs rdkafka libcjson 2>/dev/null || echo -lrdkafka -lcjson)

KAFKA_SOURCES := $(addprefix $(KAFKA_COMMONS)/,message.c ledger.c bus.c trace.c)
KAFKA_HEADERS := $(KAFKA_COMMONS)/machine_kafka.h $(KAFKA_COMMONS)/trace.h
INCLUDES := -Isrc -I$(KAFKA_COMMONS) -I$(MQTT_COMMONS) $(KAFKA_CFLAGS)

ifneq ($(strip $(MQTT_C_DIR)),)
MQTT_OBJS := build/mqtt.o build/mqtt_pal.o
MQTT_LINK :=
else
MQTT_OBJS :=
MQTT_LINK := $(MQTT_LIBS)
endif

KAFKA_BROKERS ?= localhost:9092
MQTT_HOST ?= localhost
MACHINE_ID ?= 1

.PHONY: all demo test controller test-mqtt test-kafka run clean

all: build/whirlpooling-demo

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
	$(CC) $(CPPFLAGS) $(INCLUDES) $(CFLAGS) src/main.c src/whirlpooling.c $(MQTT_COMMONS)/heartbeat.c $(KAFKA_SOURCES) $(MQTT_OBJS) $(LDFLAGS) $(MQTT_LINK) $(KAFKA_LIBS) -lm $(LDLIBS) -pthread -o $@

controller: build/whirlpooling

# Plays the scheduler (machine_kafka + experimental recipes) against the
# controller.
build/test-controller: tests/test_controller.c $(KAFKA_COMMONS)/examples/recipes.c $(KAFKA_SOURCES) $(KAFKA_HEADERS) | build
	$(CC) $(CPPFLAGS) $(INCLUDES) -I$(KAFKA_COMMONS)/examples $(CFLAGS) tests/test_controller.c $(KAFKA_COMMONS)/examples/recipes.c $(KAFKA_SOURCES) $(LDFLAGS) $(KAFKA_LIBS) -lm $(LDLIBS) -o $@

# Heartbeats against a fake MQTT broker (standalone mode, no Kafka).
test-mqtt: build/whirlpooling
	$(PYTHON) tests/test_mqtt.py ./build/whirlpooling

# start / stop / flush / restart end to end; needs Kafka and MQTT.
test-kafka: build/whirlpooling build/test-controller
	KAFKA_BROKERS=$(KAFKA_BROKERS) MQTT_HOST=$(MQTT_HOST) ./build/test-controller ./build/whirlpooling

run: build/whirlpooling
	KAFKA_BROKERS=$(KAFKA_BROKERS) ./build/whirlpooling $(MQTT_HOST) 1883 whirlpooling-$(MACHINE_ID)

clean:
	rm -rf build
