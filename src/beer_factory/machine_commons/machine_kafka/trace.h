#pragma once

/*
 * Debug trace (internal, not part of the public API).
 *
 * MK_TRACE=1 in the environment makes the library print one line per call
 * and Kafka event to stderr:
 *
 *   mk <HH:MM:SS.mmm UTC> <who> <event> key=value ...
 *
 * <who> is the client_id of a bus, "ledger:<type>#<id>" for a ledger, or "-".
 * Values with spaces are double-quoted. The format is stable so tools (and
 * the debug page in the docs) can parse it.
 */

#include "machine_kafka.h"

#include <stddef.h>

int mk_trace_enabled(void);

void mk_trace_write(const char *who, const char *event, const char *fmt, ...)
    __attribute__((format(printf, 3, 4)));

/* One-line summary of a message for trace output, e.g.
 * type=start machine=boiling#3 batch=B-1 id=... params={t:100}
 * Returns buf. */
const char *mk_trace_msg(const mk_message_t *msg, char *buf, size_t len);

#define MK_TRACE_MSG_LEN 1024

/* fmt must not be empty. */
#define MK_TRACE(who, event, ...)                                              \
  do {                                                                         \
    if (mk_trace_enabled())                                                    \
      mk_trace_write((who), (event), __VA_ARGS__);                             \
  } while (0)
