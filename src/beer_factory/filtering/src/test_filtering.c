#include "filtering.h"

#include <assert.h>
#include <stdio.h>

int main(void) {
  filtering_batches_t batches = {0};
  filtering_t filtering;
  filtering_init(&filtering, &batches);

  assert(filtering.state == FILTERING_WAIT);
  assert(filtering_start(&filtering, 10) == FILTERING_NO_BEER);
  assert(filtering_update(&filtering, 10) == FILTERING_INVALID_ARGUMENT);

  batches.beer_batches = 1;
  assert(filtering_start(&filtering, 10) == FILTERING_STARTED);
  assert(filtering.state == FILTERING_FILTER);
  assert(batches.beer_batches == 0);
  assert(filtering_start(&filtering, 10) == FILTERING_BUSY);
  assert(filtering_update(&filtering, 9) == FILTERING_INVALID_TIME);
  assert(filtering.last_time_seconds == 10);
  assert(filtering_update(&filtering, 13) == FILTERING_NO_CHANGE);
  assert(batches.finished_batches == 0);
  assert(filtering_update(&filtering, 14) == FILTERING_DONE);
  assert(filtering.state == FILTERING_WAIT);
  assert(batches.finished_batches == 1);
  assert(filtering_update(&filtering, 14) == FILTERING_INVALID_ARGUMENT);
  assert(batches.finished_batches == 1);

  batches.beer_batches = 2;
  assert(filtering_start(&filtering, 13) == FILTERING_INVALID_TIME);
  assert(batches.beer_batches == 2);
  assert(filtering_start(&filtering, 15) == FILTERING_STARTED);
  assert(filtering_update(&filtering, 20) == FILTERING_DONE);
  assert(batches.beer_batches == 1 && batches.finished_batches == 2);
  assert(filtering_start(&filtering, 20) == FILTERING_STARTED);
  assert(filtering_update(&filtering, 24) == FILTERING_DONE);
  assert(batches.beer_batches == 0 && batches.finished_batches == 3);

  puts("filtering: all tests passed");
  return 0;
}
