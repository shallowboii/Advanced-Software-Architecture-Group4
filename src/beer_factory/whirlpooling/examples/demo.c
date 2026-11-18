/* Runs one batch through the whirlpooling machine and prints each step. */
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>

#include "whirlpooling.h"

static const char *state_name(whirlpooling_state_t s) {
  return s == WHIRLPOOLING_WAIT ? "Wait" : "Whirlpool";
}

static void print(const char *event, const whirlpooling_t *w, uint64_t t) {
  printf("t=%5" PRIu64 "s  %-8s state=%-9s hot_wort=%u clarified_wort=%u\n", t,
         event, state_name(w->state), w->batches->hot_wort_batches,
         w->batches->clarified_wort_batches);
}

int main(void) {
  whirlpooling_batches_t batches = {.hot_wort_batches = 1,
                                    .clarified_wort_batches = 0};
  whirlpooling_t w;

  if (whirlpooling_init(&w, &batches) != WHIRLPOOLING_OK) return EXIT_FAILURE;
  print("init", &w, 0);

  if (whirlpooling_start(&w, 0) != WHIRLPOOLING_STARTED) return EXIT_FAILURE;
  print("start", &w, 0);

  /* Check every 15 minutes until the batch is done. */
  for (uint64_t t = 900;; t += 900) {
    whirlpooling_result_t r = whirlpooling_update(&w, t);
    if (r == WHIRLPOOLING_DONE) {
      print("done", &w, t);
      break;
    }
    if (r != WHIRLPOOLING_NO_CHANGE) return EXIT_FAILURE;
    print("running", &w, t);
  }

  return EXIT_SUCCESS;
}
