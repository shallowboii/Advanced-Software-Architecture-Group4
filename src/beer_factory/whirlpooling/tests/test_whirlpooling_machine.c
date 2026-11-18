/* Tests WHIRLPOOLING_TYPE exactly the way the scheduler drives it. */
#include <stdio.h>
#include <stdlib.h>

#include "machine.h"

extern const MachineType WHIRLPOOLING_TYPE;

#define CHECK(condition)                                                       \
  do {                                                                         \
    if (!(condition)) {                                                        \
      fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #condition);          \
      exit(EXIT_FAILURE);                                                      \
    }                                                                          \
  } while (0)

int main(void) {
  const MachineType *t = &WHIRLPOOLING_TYPE;
  void *w = calloc(1, t->size);
  CHECK(w != NULL);
  t->init(w);

  CHECK(t->count == 1);
  CHECK(t->duration(w) == 1); /* hours */

  /* Nothing queued */
  CHECK(!t->start(w));
  CHECK(!t->done(w));

  /* One batch: done exactly one tick (hour) later, and only once */
  t->add_batch(w);
  CHECK(t->start(w));
  CHECK(!t->start(w));
  CHECK(!t->done(w));
  t->tick(w);
  CHECK(t->done(w));
  CHECK(!t->done(w));

  /* Idle ticks do nothing */
  t->tick(w);
  t->tick(w);
  CHECK(!t->done(w));

  /* Two queued: second waits until the first is done */
  t->add_batch(w);
  t->add_batch(w);
  CHECK(t->start(w));
  CHECK(!t->start(w));
  t->tick(w);
  CHECK(t->done(w));
  CHECK(t->start(w));
  t->tick(w);
  CHECK(t->done(w));
  CHECK(!t->start(w));

  free(w);
  puts("whirlpooling: all machine tests passed");
  return EXIT_SUCCESS;
}
