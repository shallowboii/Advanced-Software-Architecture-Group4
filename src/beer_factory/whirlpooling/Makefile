# Use gcc unless CC is set explicitly (make's built-in default "cc" does not
# exist on MinGW/MSYS2).
ifeq ($(origin CC),default)
CC := gcc
endif
CPPFLAGS ?=
CFLAGS ?= -std=c11 -Wall -Wextra -Wpedantic -Werror -O2
LDFLAGS ?=
LDLIBS ?=

# machine.h / structure.h / greedy_scheduler.c live in the scheduling module.
SCHED_DIR := ../scheduling/src
INCLUDES := -Isrc -I$(SCHED_DIR)

CORE := src/whirlpooling.c src/whirlpooling.h
MACHINE := src/whirlpooling_machine.c $(SCHED_DIR)/machine.h

.PHONY: all demo test scheduler-check clean

all: build/whirlpooling-demo

build:
	mkdir -p $@

build/whirlpooling-demo: $(CORE) examples/demo.c | build
	$(CC) $(CPPFLAGS) $(INCLUDES) $(CFLAGS) src/whirlpooling.c examples/demo.c $(LDFLAGS) $(LDLIBS) -o $@

build/test-whirlpooling: $(CORE) tests/test_whirlpooling.c | build
	$(CC) $(CPPFLAGS) $(INCLUDES) $(CFLAGS) src/whirlpooling.c tests/test_whirlpooling.c $(LDFLAGS) $(LDLIBS) -o $@

build/test-whirlpooling-machine: $(CORE) $(MACHINE) tests/test_whirlpooling_machine.c | build
	$(CC) $(CPPFLAGS) $(INCLUDES) $(CFLAGS) src/whirlpooling.c src/whirlpooling_machine.c tests/test_whirlpooling_machine.c $(LDFLAGS) $(LDLIBS) -o $@

# greedy_scheduler.c belongs to the scheduling module, so it is compiled
# without -Werror; our own files keep the strict flags.
build/scheduler-check: $(CORE) $(MACHINE) tests/scheduler_plant.c $(SCHED_DIR)/greedy_scheduler.c $(SCHED_DIR)/structure.h | build
	$(CC) $(CPPFLAGS) $(INCLUDES) $(CFLAGS) -c src/whirlpooling.c -o build/whirlpooling.o
	$(CC) $(CPPFLAGS) $(INCLUDES) $(CFLAGS) -c src/whirlpooling_machine.c -o build/whirlpooling_machine.o
	$(CC) $(CPPFLAGS) $(INCLUDES) $(CFLAGS) -c tests/scheduler_plant.c -o build/scheduler_plant.o
	$(CC) $(CPPFLAGS) $(INCLUDES) -std=c11 -Wall -O2 -c $(SCHED_DIR)/greedy_scheduler.c -o build/greedy_scheduler.o
	$(CC) build/whirlpooling.o build/whirlpooling_machine.o build/scheduler_plant.o build/greedy_scheduler.o $(LDFLAGS) $(LDLIBS) -o $@

demo: build/whirlpooling-demo
	./build/whirlpooling-demo

test: build/test-whirlpooling build/test-whirlpooling-machine
	./build/test-whirlpooling
	./build/test-whirlpooling-machine

# Runs the real greedy scheduler for 6 batches with whirlpooling at step 4.
# Fails on any "ERROR" line or if not all 6 batches finish whirlpooling.
scheduler-check: build/scheduler-check
	./build/scheduler-check 6 > build/scheduler-check.log
	@cat build/scheduler-check.log
	@if grep -q ERROR build/scheduler-check.log; then \
		echo "scheduler-check: FAILED (ERROR lines above)"; exit 1; fi
	@n=$$(grep -c 'Whirlpooling.*done' build/scheduler-check.log); \
	if [ "$$n" -ne 6 ]; then \
		echo "scheduler-check: FAILED ($$n/6 whirlpooling batches done)"; exit 1; fi
	@echo "scheduler-check: whirlpooling works with the scheduler"

clean:
	rm -rf build
