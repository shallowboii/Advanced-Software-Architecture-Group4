#define _POSIX_C_SOURCE 200809L /* fsync, fileno */

#include "machine_kafka.h"
#include "trace.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

/*
 * File format (text, one entry per line, oldest first):
 *
 *   mk-ledger 2 <machine_type> <machine_id>
 *   <R|D|E|X> <batch_id> <command_id> <error_code|-> <error_reason...>
 *
 * R running, D done, E error, X refused without running (looked up by
 * command_id instead of batch_id).
 * Identifiers never contain spaces (see machine_kafka.h), so only the reason
 * (rest of the line) needs care: control characters are replaced by spaces.
 * The file is written to <path>.tmp, fsync'ed and renamed over <path>, so a
 * crash leaves either the old or the new version, never a mix.
 */

#define LEDGER_MAGIC "mk-ledger 2"
#define LINE_LEN 512

static int copy_str(char *dst, size_t dst_len, const char *src) {
  if (src == NULL)
    src = "";
  size_t len = strlen(src);
  if (len >= dst_len)
    return 0;
  memcpy(dst, src, len + 1);
  return 1;
}

/* Trace label of a ledger: "ledger:boiling#3". */
static const char *who(const mk_ledger_t *ledger, char *buf, size_t len) {
  snprintf(buf, len, "ledger:%s#%d", ledger->machine_type, ledger->machine_id);
  return buf;
}

#define LEDGER_TRACE(ledger, event, ...)                                       \
  do {                                                                         \
    if (mk_trace_enabled()) {                                                  \
      char who_[MK_NAME_LEN + 32];                                             \
      mk_trace_write(who((ledger), who_, sizeof(who_)), (event), __VA_ARGS__); \
    }                                                                          \
  } while (0)

static char state_char(const mk_ledger_entry_t *e) {
  if (e->refused)
    return 'X';
  return e->state == MK_DONE ? 'D' : e->state == MK_ERROR ? 'E' : 'R';
}

/* An accepted batch. */
static int find(const mk_ledger_t *ledger, const char *batch_id) {
  for (size_t i = 0; i < ledger->count; ++i) {
    const mk_ledger_entry_t *e = &ledger->entries[i];
    if (!e->refused && strcmp(e->batch_id, batch_id) == 0)
      return (int)i;
  }
  return -1;
}

/* A refused start, by its message_id. */
static int find_refused(const mk_ledger_t *ledger, const char *command_id) {
  for (size_t i = 0; i < ledger->count; ++i) {
    const mk_ledger_entry_t *e = &ledger->entries[i];
    if (e->refused && strcmp(e->command_id, command_id) == 0)
      return (int)i;
  }
  return -1;
}

static void fsync_dir(const char *path) {
  char dir[MK_PATH_LEN];
  copy_str(dir, sizeof(dir), path);
  char *slash = strrchr(dir, '/');
  if (slash == NULL)
    strcpy(dir, ".");
  else if (slash == dir)
    dir[1] = '\0';
  else
    *slash = '\0';
  int fd = open(dir, O_RDONLY);
  if (fd >= 0) {
    fsync(fd); /* best effort: makes the rename itself durable */
    close(fd);
  }
}

static mk_result_t save(const mk_ledger_t *ledger) {
  if (ledger->path[0] == '\0')
    return MK_OK;
  char tmp[MK_PATH_LEN + 8];
  snprintf(tmp, sizeof(tmp), "%s.tmp", ledger->path);
  FILE *f = fopen(tmp, "w");
  if (f == NULL) {
    fprintf(stderr, "ledger: cannot write %s: %s\n", tmp, strerror(errno));
    return MK_ERR_IO;
  }
  int ok = fprintf(f, "%s %s %d\n", LEDGER_MAGIC, ledger->machine_type,
                   ledger->machine_id) > 0;
  for (size_t i = 0; ok && i < ledger->count; ++i) {
    const mk_ledger_entry_t *e = &ledger->entries[i];
    char reason[MK_REASON_LEN];
    copy_str(reason, sizeof(reason), e->error_reason);
    for (char *c = reason; *c != '\0'; ++c) {
      if ((unsigned char)*c < 0x20 || *c == 0x7f)
        *c = ' ';
    }
    ok = fprintf(f, "%c %s %s %s %s\n", state_char(e), e->batch_id,
                 e->command_id,
                 e->error_code[0] != '\0' ? e->error_code : "-", reason) > 0;
  }
  ok = ok && fflush(f) == 0 && fsync(fileno(f)) == 0;
  ok = (fclose(f) == 0) && ok;
  if (!ok || rename(tmp, ledger->path) != 0) {
    fprintf(stderr, "ledger: cannot write %s: %s\n", ledger->path,
            strerror(errno));
    remove(tmp);
    return MK_ERR_IO;
  }
  fsync_dir(ledger->path);
  LEDGER_TRACE(ledger, "ledger.save", "path=%s entries=%zu", ledger->path,
               ledger->count);
  return MK_OK;
}

/* Splits off the next space-separated word; returns NULL if there is none. */
static char *next_word(char **cursor) {
  char *start = *cursor;
  if (start == NULL || *start == '\0')
    return NULL;
  char *space = strchr(start, ' ');
  if (space != NULL) {
    *space = '\0';
    *cursor = space + 1;
  } else {
    *cursor = start + strlen(start);
  }
  return start;
}

static int parse_entry(char *line, mk_ledger_entry_t *e) {
  memset(e, 0, sizeof(*e));
  char *cursor = line;
  char *state = next_word(&cursor), *batch = next_word(&cursor),
       *command = next_word(&cursor), *code = next_word(&cursor);
  if (state == NULL || code == NULL || strlen(state) != 1)
    return 0;
  switch (state[0]) {
  case 'R':
    e->state = MK_RUNNING;
    break;
  case 'D':
    e->state = MK_DONE;
    break;
  case 'E':
    e->state = MK_ERROR;
    break;
  case 'X':
    e->state = MK_ERROR;
    e->refused = 1;
    break;
  default:
    return 0;
  }
  if (strcmp(code, "-") == 0)
    code = "";
  return copy_str(e->batch_id, sizeof(e->batch_id), batch) &&
         copy_str(e->command_id, sizeof(e->command_id), command) &&
         copy_str(e->error_code, sizeof(e->error_code), code) &&
         copy_str(e->error_reason, sizeof(e->error_reason), cursor);
}

static mk_result_t load(mk_ledger_t *ledger) {
  FILE *f = fopen(ledger->path, "r");
  if (f == NULL) {
    if (errno == ENOENT)
      return MK_OK; /* first start of this machine */
    fprintf(stderr, "ledger: cannot read %s: %s\n", ledger->path,
            strerror(errno));
    return MK_ERR_IO;
  }

  char line[LINE_LEN];
  mk_result_t result = MK_OK;
  if (fgets(line, sizeof(line), f) == NULL) {
    result = MK_ERR_IO; /* empty: written files always have a header */
  } else {
    char expected[LINE_LEN];
    snprintf(expected, sizeof(expected), "%s %s %d\n", LEDGER_MAGIC,
             ledger->machine_type, ledger->machine_id);
    if (strncmp(line, LEDGER_MAGIC " ", sizeof(LEDGER_MAGIC)) != 0)
      result = MK_ERR_IO;
    else if (strcmp(line, expected) != 0)
      result = MK_ERR_ARG; /* the file of another machine */
  }

  while (result == MK_OK && fgets(line, sizeof(line), f) != NULL) {
    size_t len = strlen(line);
    if (len == 0 || line[len - 1] != '\n' || ledger->count == MK_LEDGER_SIZE) {
      result = MK_ERR_IO;
      break;
    }
    line[len - 1] = '\0';
    if (!parse_entry(line, &ledger->entries[ledger->count]))
      result = MK_ERR_IO;
    else
      ++ledger->count;
  }
  fclose(f);
  if (result != MK_OK)
    fprintf(stderr, "ledger: %s is %s\n", ledger->path,
            result == MK_ERR_ARG ? "for another machine" : "corrupt");
  return result;
}

mk_result_t mk_ledger_open(mk_ledger_t *ledger, const char *path,
                           const char *machine_type, int machine_id) {
  if (ledger == NULL)
    return MK_ERR_ARG;
  memset(ledger, 0, sizeof(*ledger));
  mk_message_t probe; /* validates machine_type and machine_id */
  if (mk_make_idle(&probe, machine_type, machine_id) != MK_OK ||
      !copy_str(ledger->machine_type, sizeof(ledger->machine_type),
                machine_type) ||
      !copy_str(ledger->path, sizeof(ledger->path), path))
    return MK_ERR_ARG;
  ledger->machine_id = machine_id;
  if (ledger->path[0] == '\0') {
    LEDGER_TRACE(ledger, "ledger.open", "path=(memory)");
    return MK_OK;
  }

  mk_result_t result = load(ledger);
  if (result != MK_OK) {
    LEDGER_TRACE(ledger, "ledger.open", "path=%s result=\"%s\"", ledger->path,
                 mk_result_name(result));
    ledger->count = 0;
    return result;
  }

  /* Whatever was running belonged to the previous process: never resume it
   * automatically, report it as interrupted instead. */
  int changed = 0;
  for (size_t i = 0; i < ledger->count; ++i) {
    mk_ledger_entry_t *e = &ledger->entries[i];
    if (e->state == MK_RUNNING) {
      e->state = MK_ERROR;
      e->unreported = 1;
      copy_str(e->error_code, sizeof(e->error_code), "INTERRUPTED");
      copy_str(e->error_reason, sizeof(e->error_reason),
               "machine restarted while the batch was running");
      ++changed;
    }
  }
  LEDGER_TRACE(ledger, "ledger.open", "path=%s entries=%zu interrupted=%d",
               ledger->path, ledger->count, changed);
  return changed ? save(ledger) : MK_OK;
}

static int is_for_me(const mk_ledger_t *ledger, const mk_message_t *start) {
  return start->type == MK_START && start->machine_id == ledger->machine_id &&
         strcmp(start->machine_type, ledger->machine_type) == 0 &&
         start->batch_id[0] != '\0';
}

int mk_ledger_lookup(const mk_ledger_t *ledger, const mk_message_t *start,
                     mk_message_t *resend) {
  if (ledger == NULL || start == NULL || resend == NULL ||
      !is_for_me(ledger, start))
    return MK_ERR_ARG;
  int i = find(ledger, start->batch_id);
  if (i < 0 && start->message_id[0] != '\0')
    i = find_refused(ledger, start->message_id);
  if (i < 0) {
    LEDGER_TRACE(ledger, "ledger.lookup", "batch=%s id=%s result=new",
                 start->batch_id, start->message_id);
    return 0;
  }
  const mk_ledger_entry_t *e = &ledger->entries[i];
  LEDGER_TRACE(ledger, "ledger.lookup",
               "batch=%s id=%s result=duplicate state=%s first_id=%s",
               start->batch_id, start->message_id,
               e->refused ? "refused" : mk_type_name(e->state), e->command_id);
  mk_result_t result =
      e->state == MK_DONE    ? mk_make_done(resend, start)
      : e->state == MK_ERROR ? mk_make_error(resend, start, e->error_code,
                                             e->error_reason)
                             : mk_make_running(resend, start);
  return result == MK_OK ? 1 : result;
}

/* Adds an entry (dropping the oldest when full) and writes the file. On
 * failure the ledger is left unchanged. */
static mk_result_t append(mk_ledger_t *ledger, const mk_ledger_entry_t *entry) {
  mk_ledger_t before = *ledger;
  if (ledger->count == MK_LEDGER_SIZE) { /* forget the oldest entry */
    memmove(&ledger->entries[0], &ledger->entries[1],
            (MK_LEDGER_SIZE - 1) * sizeof(ledger->entries[0]));
    --ledger->count;
  }
  ledger->entries[ledger->count++] = *entry;
  mk_result_t result = save(ledger);
  if (result != MK_OK)
    *ledger = before;
  return result;
}

static int entry_from(mk_ledger_entry_t *e, const mk_message_t *start) {
  memset(e, 0, sizeof(*e));
  return copy_str(e->batch_id, sizeof(e->batch_id), start->batch_id) &&
         copy_str(e->command_id, sizeof(e->command_id), start->message_id);
}

mk_result_t mk_ledger_accept(mk_ledger_t *ledger, const mk_message_t *start) {
  if (ledger == NULL || start == NULL || !is_for_me(ledger, start) ||
      start->message_id[0] == '\0' || find(ledger, start->batch_id) >= 0)
    return MK_ERR_ARG;
  mk_ledger_entry_t e;
  if (!entry_from(&e, start))
    return MK_ERR_ARG;
  e.state = MK_RUNNING;
  /* Not durable on failure: the caller must not run the batch. */
  mk_result_t result = append(ledger, &e);
  LEDGER_TRACE(ledger, "ledger.accept", "batch=%s id=%s result=\"%s\"",
               start->batch_id, start->message_id, mk_result_name(result));
  return result;
}

mk_result_t mk_ledger_refuse(mk_ledger_t *ledger, const mk_message_t *start,
                             const char *code, const char *reason,
                             mk_message_t *status) {
  if (ledger == NULL || start == NULL || status == NULL ||
      !is_for_me(ledger, start) || start->message_id[0] == '\0' ||
      find(ledger, start->batch_id) >= 0)
    return MK_ERR_ARG;
  mk_message_t error;
  mk_result_t result = mk_make_error(&error, start, code, reason);
  if (result != MK_OK)
    return result;
  if (find_refused(ledger, start->message_id) < 0) {
    mk_ledger_entry_t e;
    if (!entry_from(&e, start))
      return MK_ERR_ARG;
    e.state = MK_ERROR;
    e.refused = 1;
    memcpy(e.error_code, error.error_code, sizeof(e.error_code));
    memcpy(e.error_reason, error.error_reason, sizeof(e.error_reason));
    result = append(ledger, &e);
    LEDGER_TRACE(ledger, "ledger.refuse", "batch=%s id=%s error=%s result=\"%s\"",
                 start->batch_id, start->message_id, error.error_code,
                 mk_result_name(result));
    if (result != MK_OK)
      return result;
  }
  *status = error;
  return MK_OK;
}

mk_result_t mk_ledger_record(mk_ledger_t *ledger, const mk_message_t *status) {
  if (ledger == NULL || status == NULL || mk_is_command(status->type))
    return MK_ERR_ARG;
  if ((status->type != MK_DONE && status->type != MK_ERROR) ||
      status->batch_id[0] == '\0')
    return MK_OK; /* not about a batch (e.g. an error answering stop) */
  int i = find(ledger, status->batch_id);
  if (i < 0)
    return MK_OK; /* never accepted: nothing to remember */
  mk_ledger_entry_t *e = &ledger->entries[i];
  e->state = status->type;
  e->unreported = 0;
  copy_str(e->error_code, sizeof(e->error_code), status->error_code);
  copy_str(e->error_reason, sizeof(e->error_reason), status->error_reason);
  LEDGER_TRACE(ledger, "ledger.record", "batch=%s state=%s%s%s",
               status->batch_id, mk_type_name(status->type),
               status->error_code[0] != '\0' ? " error=" : "",
               status->error_code);
  return save(ledger);
}

int mk_ledger_next_interrupted(mk_ledger_t *ledger, mk_message_t *status) {
  if (ledger == NULL || status == NULL)
    return 0;
  for (size_t i = 0; i < ledger->count; ++i) {
    mk_ledger_entry_t *e = &ledger->entries[i];
    if (!e->unreported)
      continue;
    e->unreported = 0;
    /* Rebuild the start this entry came from, then answer it. */
    mk_message_t start;
    if (mk_make_start(&start, ledger->machine_type, ledger->machine_id,
                      e->batch_id) != MK_OK ||
        mk_make_error(status, &start, e->error_code, e->error_reason) != MK_OK)
      continue;
    LEDGER_TRACE(ledger, "ledger.interrupted", "batch=%s first_id=%s",
                 e->batch_id, e->command_id);
    return 1;
  }
  return 0;
}
