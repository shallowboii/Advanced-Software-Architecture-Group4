#include "whirlpooling.h"

#include <stddef.h>

whirlpooling_result_t whirlpooling_init(whirlpooling_t *whirlpooling,
                                        whirlpooling_batches_t *batches) {
  if (whirlpooling == NULL || batches == NULL) {
    return WHIRLPOOLING_INVALID_ARGUMENT;
  }

  *whirlpooling = (whirlpooling_t){.state = WHIRLPOOLING_WAIT,
                                   .batches = batches,
                                   .duration_seconds =
                                       WHIRLPOOLING_DURATION_SECONDS,
                                   .started_at_seconds = 0,
                                   .last_time_seconds = 0};

  return WHIRLPOOLING_OK;
}

whirlpooling_result_t whirlpooling_set_duration(whirlpooling_t *whirlpooling,
                                                uint64_t seconds) {
  if (whirlpooling == NULL || seconds == 0) {
    return WHIRLPOOLING_INVALID_ARGUMENT;
  }

  if (whirlpooling->state != WHIRLPOOLING_WAIT) {
    return WHIRLPOOLING_BUSY;
  }

  whirlpooling->duration_seconds = seconds;
  return WHIRLPOOLING_OK;
}

whirlpooling_result_t whirlpooling_start(whirlpooling_t *whirlpooling,
                                         uint64_t now_seconds) {
  if (whirlpooling == NULL || whirlpooling->batches == NULL) {
    return WHIRLPOOLING_INVALID_ARGUMENT;
  }

  /* Time must not go back, and the deadline must be representable. */
  if (now_seconds < whirlpooling->last_time_seconds ||
      now_seconds > UINT64_MAX - whirlpooling->duration_seconds) {
    return WHIRLPOOLING_INVALID_TIME;
  }

  if (whirlpooling->state != WHIRLPOOLING_WAIT) {
    return WHIRLPOOLING_BUSY;
  }

  /* Guard: has_hot_wort() */
  if (whirlpooling->batches->hot_wort_batches < 1) {
    return WHIRLPOOLING_NO_HOT_WORT;
  }

  /* Assignments: reset(), take_hot_wort() */
  whirlpooling->started_at_seconds = now_seconds;
  whirlpooling->last_time_seconds = now_seconds;
  whirlpooling->batches->hot_wort_batches--;
  whirlpooling->state = WHIRLPOOLING_WHIRLPOOL;

  return WHIRLPOOLING_STARTED;
}

whirlpooling_result_t whirlpooling_update(whirlpooling_t *whirlpooling,
                                          uint64_t now_seconds) {
  if (whirlpooling == NULL || whirlpooling->batches == NULL) {
    return WHIRLPOOLING_INVALID_ARGUMENT;
  }

  if (now_seconds < whirlpooling->last_time_seconds) {
    return WHIRLPOOLING_INVALID_TIME;
  }

  whirlpooling->last_time_seconds = now_seconds;

  if (whirlpooling->state != WHIRLPOOLING_WHIRLPOOL) {
    return WHIRLPOOLING_NO_CHANGE;
  }

  /* Guard: a >= T_WHIRLPOOLING (subtraction cannot overflow here). */
  if (now_seconds - whirlpooling->started_at_seconds <
      whirlpooling->duration_seconds) {
    return WHIRLPOOLING_NO_CHANGE;
  }

  /* Assignment: make_clarified_wort() */
  whirlpooling->batches->clarified_wort_batches++;
  whirlpooling->state = WHIRLPOOLING_WAIT;

  return WHIRLPOOLING_DONE;
}

whirlpooling_result_t whirlpooling_stop(whirlpooling_t *whirlpooling) {
  if (whirlpooling == NULL) {
    return WHIRLPOOLING_INVALID_ARGUMENT;
  }

  if (whirlpooling->state != WHIRLPOOLING_WHIRLPOOL) {
    return WHIRLPOOLING_NOT_RUNNING;
  }

  whirlpooling->state = WHIRLPOOLING_STOPPED;
  return WHIRLPOOLING_OK;
}

whirlpooling_result_t whirlpooling_flush(whirlpooling_t *whirlpooling) {
  if (whirlpooling == NULL) {
    return WHIRLPOOLING_INVALID_ARGUMENT;
  }

  if (whirlpooling->state == WHIRLPOOLING_WAIT) {
    return WHIRLPOOLING_NOT_RUNNING;
  }

  whirlpooling->state = WHIRLPOOLING_WAIT; /* the wort is discarded */
  return WHIRLPOOLING_OK;
}
