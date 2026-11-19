#define _POSIX_C_SOURCE 200809L /* clock_gettime, gmtime_r */

#include "trace.h"

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static int trace_state = -1; /* -1: MK_TRACE not read yet */

int mk_trace_enabled(void) {
  if (trace_state < 0) {
    const char *v = getenv("MK_TRACE");
    trace_state = v != NULL && v[0] != '\0' && strcmp(v, "0") != 0;
  }
  return trace_state;
}

void mk_trace_write(const char *who, const char *event, const char *fmt, ...) {
  struct timespec now;
  struct tm utc;
  char clock[48] = "??:??:??.???";
  if (clock_gettime(CLOCK_REALTIME, &now) == 0 &&
      gmtime_r(&now.tv_sec, &utc) != NULL)
    snprintf(clock, sizeof(clock), "%02d:%02d:%02d.%03d", utc.tm_hour % 24,
             utc.tm_min % 60, utc.tm_sec % 61,
             (int)(now.tv_nsec / 1000000L % 1000));

  /* One fprintf per line keeps lines whole when several processes share a
   * terminal. */
  char line[2048];
  int n = snprintf(line, sizeof(line), "mk %s %s %s ", clock,
                   who != NULL && who[0] != '\0' ? who : "-", event);
  if (n < 0 || (size_t)n >= sizeof(line))
    n = 0;
  va_list ap;
  va_start(ap, fmt);
  vsnprintf(line + n, sizeof(line) - (size_t)n, fmt, ap);
  va_end(ap);
  fprintf(stderr, "%s\n", line);
}

/* Appends to buf at *pos without overflowing; output is cut if too long. */
static void append(char *buf, size_t len, size_t *pos, const char *fmt, ...)
    __attribute__((format(printf, 4, 5)));

static void append(char *buf, size_t len, size_t *pos, const char *fmt, ...) {
  if (*pos >= len)
    return;
  va_list ap;
  va_start(ap, fmt);
  int n = vsnprintf(buf + *pos, len - *pos, fmt, ap);
  va_end(ap);
  if (n > 0)
    *pos += (size_t)n < len - *pos ? (size_t)n : len - *pos - 1;
}

const char *mk_trace_msg(const mk_message_t *m, char *buf, size_t len) {
  if (buf == NULL || len == 0)
    return "";
  buf[0] = '\0';
  if (m == NULL)
    return buf;
  size_t pos = 0;
  append(buf, len, &pos, "type=%s machine=%s#%d", mk_type_name(m->type),
         m->machine_type[0] != '\0' ? m->machine_type : "?", m->machine_id);
  if (m->batch_id[0] != '\0')
    append(buf, len, &pos, " batch=%s", m->batch_id);
  if (m->message_id[0] != '\0')
    append(buf, len, &pos, " id=%s", m->message_id);
  if (m->ts_utc[0] != '\0')
    append(buf, len, &pos, " ts=%s", m->ts_utc);
  if (m->n_params > 0 && m->n_params <= MK_MAX_PARAMS) {
    append(buf, len, &pos, " params={");
    for (size_t i = 0; i < m->n_params; ++i)
      append(buf, len, &pos, "%s%s:%g", i > 0 ? "," : "", m->params[i].name,
             m->params[i].value);
    append(buf, len, &pos, "}");
  }
  if (m->error_code[0] != '\0')
    append(buf, len, &pos, " error=%s reason=\"%s\"", m->error_code,
           m->error_reason);
  return buf;
}
