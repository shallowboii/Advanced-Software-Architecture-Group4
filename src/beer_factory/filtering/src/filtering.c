#include "filtering.h"

void filtering_init(filtering_t *filtering, filtering_batches_t *batches) {
  *filtering = (filtering_t){.state = FILTERING_WAIT, .batches = batches};
}

filtering_result_t filtering_start(filtering_t *filtering, uint64_t now_seconds) {
  if (now_seconds < filtering->last_time_seconds) {
    return FILTERING_INVALID_TIME;
  }

  if (filtering->state != FILTERING_WAIT) {
    return FILTERING_BUSY;
  }

  if (filtering->batches->beer_batches < 1) {
    return FILTERING_NO_BEER;
  }

  filtering->started_at_seconds = now_seconds;
  filtering->last_time_seconds = now_seconds;
  filtering->batches->beer_batches--;
  filtering->state = FILTERING_FILTER;

  return FILTERING_STARTED;
}

filtering_result_t filtering_stop(filtering_t *filtering) {
  filtering->state = FILTERING_WAIT;
  return FILTERING_DONE;
}

filtering_result_t filtering_flush(filtering_t *filtering) {
  filtering->state = FILTERING_WAIT;
  return FILTERING_DONE;
}

filtering_result_t filtering_update(filtering_t *filtering, uint64_t now_seconds) {
  if (filtering->state != FILTERING_FILTER) {
    return FILTERING_INVALID_ARGUMENT;
  }

  if (now_seconds < filtering->last_time_seconds) {
    return FILTERING_INVALID_TIME;
  }

  filtering->last_time_seconds = now_seconds;
  if (now_seconds - filtering->started_at_seconds < FILTERING_DURATION_SECONDS) {
    return FILTERING_NO_CHANGE;
  }

  filtering->batches->finished_batches++;
  filtering->state = FILTERING_WAIT;

  return FILTERING_DONE;
}
