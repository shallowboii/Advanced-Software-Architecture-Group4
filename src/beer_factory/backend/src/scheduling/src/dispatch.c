#include "scheduler.h"

/*
 * The scheduling policy goes here.
 *
 * Called after every status with the machine row that changed. Everything
 * the scheduler knows is in scheduler_machines(s); work is sent with
 * scheduler_start (recipe values as params, e.g. duration_s),
 * scheduler_stop and scheduler_flush.
 *
 * Typical triggers:
 *   changed->state == MK_IDLE   machine is free: start the next batch on it
 *   changed->state == MK_DONE   batch finished: start its next step elsewhere
 *   changed->state == MK_ERROR  e.g. INTERRUPTED, BUSY: decide what to do
 */
void dispatch(scheduler_t *s, const machine_t *changed) {
  (void)s;
  (void)changed;
}
