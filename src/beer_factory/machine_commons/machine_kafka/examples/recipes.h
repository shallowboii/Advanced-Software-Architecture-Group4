#pragma once

/*
 * EXPERIMENTAL / TEMPORARY. Only here so the whirlpooling controller and the
 * example scheduler can be tested with Beer A / Beer B parameters. Where
 * recipes are really defined, versioned and turned into start parameters
 * (backend recipe versions, the scheduler, a shared fixture file?) is still
 * open; once that is decided this file goes away. Machines don't depend on
 * it: they only read the params of the start they receive.
 *
 * Example recipes for the scheduler side (FR: "a supervisor selects Beer A or
 * Beer B for a new batch; every stage controller receives that beer's
 * parameters").
 *
 * Machines have no "change recipe" function. The recipe of a batch travels
 * with every start: when the scheduler starts batch B-7 (Beer B) on a
 * machine, it copies the Beer B parameters for that machine type into the
 * start's params, and the machine reads them with mk_param_get. Batches that
 * are already running keep the parameters of their own start; the next batch
 * can use another beer. In the real system the table below comes from the
 * backend's recipe versions, not from C code.
 *
 * Durations follow IMPLEMENTATION_PLAN.md (Beer A = Ale, Beer B = Lager);
 * temperatures and amounts are example values.
 */

#include "machine_kafka.h"

/* Copies the parameters of beer ("A" or "B") for start->machine_type into
 * the start. MK_ERR_ARG for an unknown beer or machine type. */
mk_result_t recipe_apply(mk_message_t *start, const char *beer);

/* "Ale" / "Lager", or NULL for an unknown beer. */
const char *recipe_name(const char *beer);
