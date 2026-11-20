/* EXPERIMENTAL / TEMPORARY recipe table, see recipes.h. */
#include "recipes.h"

#include <stddef.h>
#include <string.h>

typedef struct {
  const char *name;
  double value;
} param_t;

typedef struct {
  const char *beer;         /* "A" or "B" */
  const char *machine_type; /* topic */
  param_t params[6];        /* ends at the first NULL name */
} recipe_step_t;

#define H 3600.0 /* seconds per hour */

static const recipe_step_t RECIPES[] = {
    /* Beer A: Ale */
    {"A", "mashing", {{"target_temp_c", 65}, {"duration_s", 2 * H}, {"speed_rpm", 30}}},
    {"A", "lautering", {{"water_temp_c", 78}, {"duration_s", 2 * H}}},
    {"A", "boiling", {{"target_temp_c", 100}, {"duration_s", 1 * H}, {"hops_g", 250}, {"hops_at_s", 600}}},
    {"A", "whirlpooling", {{"speed_rpm", 60}, {"duration_s", 1 * H}}},
    {"A", "fermentation", {{"fermentation_temp_c", 19}, {"fermentation_s", 168 * H}, {"maturation_temp_c", 4}, {"maturation_s", 168 * H}, {"yeast_g", 500}}},
    {"A", "filtering", {{"target_temp_c", 2}, {"co2_volumes", 2.4}, {"duration_s", 2 * H}}},
    /* Beer B: Lager */
    {"B", "mashing", {{"target_temp_c", 62}, {"duration_s", 2 * H}, {"speed_rpm", 30}}},
    {"B", "lautering", {{"water_temp_c", 76}, {"duration_s", 2 * H}}},
    {"B", "boiling", {{"target_temp_c", 100}, {"duration_s", 1 * H}, {"hops_g", 180}, {"hops_at_s", 900}}},
    {"B", "whirlpooling", {{"speed_rpm", 45}, {"duration_s", 1 * H}}},
    {"B", "fermentation", {{"fermentation_temp_c", 10}, {"fermentation_s", 336 * H}, {"maturation_temp_c", 1}, {"maturation_s", 504 * H}, {"yeast_g", 800}}},
    {"B", "filtering", {{"target_temp_c", 0}, {"co2_volumes", 2.6}, {"duration_s", 2 * H}}},
};

const char *recipe_name(const char *beer) {
  if (beer == NULL)
    return NULL;
  if (strcmp(beer, "A") == 0)
    return "Ale";
  if (strcmp(beer, "B") == 0)
    return "Lager";
  return NULL;
}

mk_result_t recipe_apply(mk_message_t *start, const char *beer) {
  if (start == NULL || start->type != MK_START || recipe_name(beer) == NULL)
    return MK_ERR_ARG;
  for (size_t i = 0; i < sizeof(RECIPES) / sizeof(RECIPES[0]); ++i) {
    const recipe_step_t *r = &RECIPES[i];
    if (strcmp(r->beer, beer) != 0 ||
        strcmp(r->machine_type, start->machine_type) != 0)
      continue;
    for (size_t p = 0; p < 6 && r->params[p].name != NULL; ++p) {
      mk_result_t result =
          mk_param_set(start, r->params[p].name, r->params[p].value);
      if (result != MK_OK)
        return result;
    }
    return MK_OK;
  }
  return MK_ERR_ARG;
}
