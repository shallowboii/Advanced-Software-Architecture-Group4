/*
 * Adapter that exposes the whirlpooling state machine through the
 * scheduler's MachineType interface (scheduling/src/machine.h).
 * The scheduler counts in hours; the core counts in seconds.
 */
#include <stdbool.h>

#include "machine.h"
#include "whirlpooling.h"

#define SECONDS_PER_HOUR 3600

typedef struct {
  whirlpooling_t core;
  whirlpooling_batches_t batches; /* core.batches points here */
  uint64_t now_seconds;
} WhirlpoolingMachine;

static void init(void *self) {
  WhirlpoolingMachine *m = self;
  m->batches = (whirlpooling_batches_t){0, 0};
  m->now_seconds = 0;
  (void)whirlpooling_init(&m->core, &m->batches);
}

static int duration(const void *self) {
  (void)self;
  return WHIRLPOOLING_DURATION_SECONDS / SECONDS_PER_HOUR;
}

/* A hot-wort batch arrives from boiling. */
static void add_batch(void *self) {
  ((WhirlpoolingMachine *)self)->batches.hot_wort_batches++;
}

static bool start(void *self) {
  WhirlpoolingMachine *m = self;
  return whirlpooling_start(&m->core, m->now_seconds) == WHIRLPOOLING_STARTED;
}

static bool done(void *self) {
  WhirlpoolingMachine *m = self;
  return whirlpooling_update(&m->core, m->now_seconds) == WHIRLPOOLING_DONE;
}

/* One scheduler tick = one hour. */
static void tick(void *self) {
  ((WhirlpoolingMachine *)self)->now_seconds += SECONDS_PER_HOUR;
}

const MachineType WHIRLPOOLING_TYPE = {
    "4 Whirlpooling", 1, sizeof(WhirlpoolingMachine),
    init, duration, add_batch, start, done, tick};
