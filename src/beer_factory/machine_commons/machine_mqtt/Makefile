CC = cc
AR = ar
CFLAGS = -std=c11 -Wall -Wextra -Werror -pedantic
CPPFLAGS += -D_POSIX_C_SOURCE=200809L
THREAD_FLAGS = -pthread

# Redirect network calls only in the test copy of the wrapper.
TEST_STUBS = -Dgetaddrinfo=test_getaddrinfo -Dfreeaddrinfo=test_freeaddrinfo \
	-Dsocket=test_socket -Dfcntl=test_fcntl -Dconnect=test_connect \
	-Dpoll=test_poll -Dgetsockopt=test_getsockopt -Dclose=test_close \
	-Dpthread_mutex_destroy=test_pthread_mutex_destroy

.PHONY: all test

all: build/libheartbeat.a

build:
	mkdir -p $@

build/heartbeat.o: heartbeat.c heartbeat.h mqtt.h mqtt_pal.h Makefile | build
	$(CC) $(CPPFLAGS) $(CFLAGS) $(THREAD_FLAGS) -c heartbeat.c -o $@

build/libheartbeat.a: build/heartbeat.o
	$(AR) rcs $@ $<

build/heartbeat-test.o: heartbeat.c heartbeat.h mqtt.h mqtt_pal.h Makefile | build
	$(CC) $(CPPFLAGS) $(CFLAGS) $(THREAD_FLAGS) $(TEST_STUBS) -c heartbeat.c -o $@

build/test_heartbeat.o: tests/test_heartbeat.c heartbeat.h mqtt.h mqtt_pal.h Makefile | build
	$(CC) $(CPPFLAGS) -I. $(CFLAGS) $(THREAD_FLAGS) -UNDEBUG -c $< -o $@

build/test-heartbeat: build/heartbeat-test.o build/test_heartbeat.o
	$(CC) $(CFLAGS) $(THREAD_FLAGS) $(LDFLAGS) $^ $(LDLIBS) -o $@

test: build/test-heartbeat
	./build/test-heartbeat
