CC = cc
AR = ar
CFLAGS = -std=c11 -Wall -Wextra -Werror -pedantic

.PHONY: all test

all: build/libheartbeat.a

build:
	mkdir -p $@

build/heartbeat.o: heartbeat.c heartbeat.h mqtt.h mqtt_pal.h | build
	$(CC) $(CPPFLAGS) $(CFLAGS) -c heartbeat.c -o $@

build/libheartbeat.a: build/heartbeat.o
	$(AR) rcs $@ $<

build/test-heartbeat: heartbeat.c heartbeat.h mqtt.h mqtt_pal.h tests/test_heartbeat.c | build
	$(CC) $(CPPFLAGS) -I. $(CFLAGS) heartbeat.c tests/test_heartbeat.c $(LDFLAGS) $(LDLIBS) -o $@

test: build/test-heartbeat
	./build/test-heartbeat
