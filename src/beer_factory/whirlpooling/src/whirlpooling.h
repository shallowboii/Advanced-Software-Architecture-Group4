#pragma once

#include <stdint.h>

/*
 * Whirlpooling machine, mirroring the Whirlpooling template in
 * src/docs/Beer_Factory.xml:
 *
 *   Wait --[hot_wort_batches >= 1] start_whirlpool? / a = 0,
 *          hot_wort_batches-- --> Whirlpool
 *   Whirlpool (invariant a <= T_WHIRLPOOLING)
 *   Whirlpool --[a >= T_WHIRLPOOLING] done_whirlpool! /
 *          clarified_wort_batches++ --> Wait
 *
 * Time is absolute, in seconds, and is passed in by the caller.
 */

/* T_WHIRLPOOLING = 1 h (FR-10.4); a start can set another run time
 * (whirlpooling_set_duration). */
#ifndef TIME_DIVISOR
#define TIME_DIVISOR 1
#endif

#define WHIRLPOOLING_DURATION_SECONDS (3600 / TIME_DIVISOR)

/*
 * STOPPED is not in the UPPAAL model: it is the stop command (FR-13). The
 * whirlpool stops spinning, the wort stays in the vessel until it is flushed
 * (FR-14), and no clarified wort is produced.
 */
typedef enum {
  WHIRLPOOLING_WAIT,      /* UPPAAL location "Wait" */
  WHIRLPOOLING_WHIRLPOOL, /* UPPAAL location "Whirlpooling" */
  WHIRLPOOLING_STOPPED    /* stopped by command, batch still inside */
} whirlpooling_state_t;

/* The two shared counters passed to Whirlpooling in Beer_Factory.xml. */
typedef struct {
  unsigned int hot_wort_batches;       /* input, produced by Boiling */
  unsigned int clarified_wort_batches; /* output, consumed by the tanks */
} whirlpooling_batches_t;

typedef enum {
  WHIRLPOOLING_OK,
  WHIRLPOOLING_NO_CHANGE,
  WHIRLPOOLING_STARTED,
  WHIRLPOOLING_DONE,
  WHIRLPOOLING_BUSY,
  WHIRLPOOLING_NO_HOT_WORT,
  WHIRLPOOLING_INVALID_ARGUMENT,
  WHIRLPOOLING_INVALID_TIME,
  WHIRLPOOLING_NOT_RUNNING /* stop: nothing running; flush: vessel empty */
} whirlpooling_result_t;

typedef struct {
  whirlpooling_state_t state;
  whirlpooling_batches_t *batches;
  uint64_t duration_seconds;   /* T_WHIRLPOOLING of the current batch */
  uint64_t started_at_seconds; /* when clock "a" was reset */
  uint64_t last_time_seconds;  /* latest time seen, to reject going back */
} whirlpooling_t;

whirlpooling_result_t whirlpooling_init(whirlpooling_t *whirlpooling,
                                        whirlpooling_batches_t *batches);

/* Run time of the next batch (start parameter duration_s). Only while WAIT. */
whirlpooling_result_t whirlpooling_set_duration(whirlpooling_t *whirlpooling,
                                                uint64_t seconds);

/* start_whirlpool?: takes one hot-wort batch and starts the run. */
whirlpooling_result_t whirlpooling_start(whirlpooling_t *whirlpooling,
                                         uint64_t now_seconds);

/*
 * Advances time. At/after the one-hour deadline, returns WHIRLPOOLING_DONE
 * once, produces one clarified-wort batch and returns to WHIRLPOOLING_WAIT.
 * While waiting it returns WHIRLPOOLING_NO_CHANGE.
 */
whirlpooling_result_t whirlpooling_update(whirlpooling_t *whirlpooling,
                                          uint64_t now_seconds);

/* Stop (FR-13): WHIRLPOOL -> STOPPED; the wort stays inside. NOT_RUNNING if
 * nothing is running. */
whirlpooling_result_t whirlpooling_stop(whirlpooling_t *whirlpooling);

/* Flush (FR-14): discard the batch in the vessel (running or stopped),
 * -> WAIT, no clarified wort. NOT_RUNNING if the vessel is empty. */
whirlpooling_result_t whirlpooling_flush(whirlpooling_t *whirlpooling);
