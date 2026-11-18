/*
 * Plant for the scheduler integration check: the real WHIRLPOOLING_TYPE at
 * step 4, stand-in machines for the other steps (same stubs as
 * scheduling/src/test_plant.c). Linked with scheduling/src/greedy_scheduler.c.
 */
#include "structure.h"

extern const MachineType WHIRLPOOLING_TYPE;

typedef struct { int busy, c, duration, queued; } Stub;

static int stub_duration(const void *s) { return ((const Stub *)s)->duration; }
static void stub_add(void *s) { ((Stub *)s)->queued++; }

static bool stub_start(void *s) {
  Stub *m = s;
  if (!m->busy && m->queued > 0) {
    m->queued--;
    m->busy = 1;
    m->c = 0;
    return true;
  }
  return false;
}

static bool stub_done(void *s) {
  Stub *m = s;
  if (m->busy && m->c >= m->duration) {
    m->busy = 0;
    return true;
  }
  return false;
}

static void stub_tick(void *s) {
  Stub *m = s;
  if (m->busy) m->c++;
}

#define STUB_TYPE(ID, NAME, T, COUNT)                                          \
  static void ID##_init(void *s) {                                             \
    Stub *m = s;                                                               \
    m->busy = 0;                                                               \
    m->c = 0;                                                                  \
    m->queued = 0;                                                             \
    m->duration = T;                                                           \
  }                                                                            \
  static const MachineType ID = {NAME, COUNT, sizeof(Stub), ID##_init,         \
                                 stub_duration, stub_add, stub_start,          \
                                 stub_done, stub_tick}

STUB_TYPE(S_MASHING, "1 Mashing", 3, 1);
STUB_TYPE(S_LAUTERING, "2 Lautering", 2, 1);
STUB_TYPE(S_BOILING, "3 Boiling", 2, 1);
STUB_TYPE(S_FERMENTATION, "5 Fermentation", 168, 4);
STUB_TYPE(S_FILTERING, "6 Filtering", 4, 1);

const MachineType *const PLANT[NUM_STEPS] = {
    &S_MASHING,        &S_LAUTERING,    &S_BOILING,
    &WHIRLPOOLING_TYPE, &S_FERMENTATION, &S_FILTERING,
};
