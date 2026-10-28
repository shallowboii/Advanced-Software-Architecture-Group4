#define _POSIX_C_SOURCE 200809L
#include "machine.h"

#include <errno.h>
#include <fcntl.h>
#include <math.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* The main thread alone owns execution state and the RAM-only message lists.
   A process restart deliberately forgets all state; this is a simulator, not a
   crash-recoverable industrial controller. MQTT disconnects do not reset RAM. */

struct BFPendingEvent {
    char event_id[BF_ID];
    uint64_t sequence;
    char *payload;
    struct BFPendingEvent *next;
};

struct BFCommandResult {
    char command_id[BF_ID];
    char event_id[BF_ID];
    uint64_t sequence;
    char *fingerprint;
    char *response;
    struct BFCommandResult *next;
};

typedef struct {
    char *buf;
    size_t cap, n;
    int failed;
} Writer;
/** Append formatted JSON without writing past the output buffer; later writes stop on overflow. */
static void json_append(Writer *w, const char *format, ...) {
    if (w->failed)
        return;
    va_list args;
    va_start(args, format);
    int n = vsnprintf(w->buf + w->n, w->cap - w->n, format, args);
    va_end(args);
    if (n < 0 || (size_t)n >= w->cap - w->n)
        w->failed = 1;
    else
        w->n += (size_t)n;
}
/** Escape a string as one JSON value, including quotes, backslashes and control bytes. */
static void json_write_string(Writer *w, const char *s) {
    json_append(w, "\"");
    for (const unsigned char *p = (const unsigned char *)s; *p; p++) {
        if (*p == '"' || *p == '\\')
            json_append(w, "\\%c", *p);
        else if (*p < 32)
            json_append(w, "\\u%04x", *p);
        else
            json_append(w, "%c", *p);
    }
    json_append(w, "\"");
}
/** Expose bounded JSON string encoding to the telemetry transport. */
int bf_json_string(const char *value, char *out, size_t cap) {
    Writer w = {out, cap, 0, 0};
    json_write_string(&w, value);
    return w.failed ? -1 : 0;
}
/** Append a nullable string field; an empty internal identifier becomes JSON null. */
static void json_optional_field(Writer *w, const char *key, const char *value) {
    json_append(w, ",\"%s\":", key);
    if (*value)
        json_write_string(w, value);
    else
        json_append(w, "null");
}
/** Copy a nullable string into a fixed-size, always-terminated destination. */
static void copy_text(char *to, size_t n, const char *from) {
    snprintf(to, n, "%s", from ? from : "");
}
/** Format wall-clock time for human-readable event timestamps; timers use monotonic time instead.
 */
void bf_utc(double wall, char *out, size_t cap) {
    time_t sec = (time_t)wall;
    struct tm t;
    gmtime_r(&sec, &t);
    strftime(out, cap, "%Y-%m-%dT%H:%M:%SZ", &t);
}
/** Create a random UUID for event and boot identity; failure is fatal rather than reusing an ID. */
void bf_uuid(char *out, size_t cap) {
    unsigned char bytes[16];
    int fd = open("/dev/urandom", O_RDONLY);
    ssize_t n = fd >= 0 ? read(fd, bytes, sizeof bytes) : -1;
    if (fd >= 0)
        close(fd);
    if (n != (ssize_t)sizeof bytes)
        abort();
    bytes[6] = (unsigned char)((bytes[6] & 15) | 64);
    bytes[8] = (unsigned char)((bytes[8] & 63) | 128);
    snprintf(out, cap, "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
             bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
             bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]);
}
/** Accept only the six machine types from the shared brewing-line protocol. */
int bf_valid_type(const char *type) {
    const char *types[] = {"mash", "lauter", "kettle", "whirlpool", "unitank", "filter"};
    for (size_t i = 0; i < sizeof types / sizeof *types; i++)
        if (!strcmp(types[i], type))
            return 1;
    return 0;
}
/** Allow material work only while the lease is valid and both stop latches are clear. */
int bf_permitted(const BFMachine *m, double mono) {
    return mono < m->permit_until && !m->s.local_stop && !m->s.global_stop;
}
/** Serialize equipment observations; versions change only on structural transitions. */
int bf_snapshot(const BFMachine *m, double mono, char *out, size_t capacity) {
    const BFState *s = &m->s;
    Writer w = {out, capacity, 0, 0};
    char utc[40];
    bf_utc(s->last_wall, utc, sizeof utc);
    json_append(&w, "{\"schema_version\":2,\"machine_id\":");
    json_write_string(&w, s->machine_id);
    json_optional_field(&w, "type", s->type);
    json_optional_field(&w, "controller_boot_id", s->boot_id);
    json_optional_field(&w, "state", s->state);
    json_optional_field(&w, "current_command_id", s->current_command_id);
    json_optional_field(&w, "interrupted_command_id", s->interrupted_command_id);
    json_optional_field(&w, "transfer_role", s->transfer_role);
    json_optional_field(&w, "ts_utc", utc);
    json_append(&w,
                ",\"occupied\":%s,\"duration_h\":%.17g,\"elapsed_h\":%.17g,\"target_temp_c\":%.17g",
                s->occupied ? "true" : "false", s->duration_h, s->elapsed_h, s->target_temp_c);
    json_append(
        &w, ",\"transfer_ready\":%s,\"flush_requested\":%s,\"local_stop\":%s,\"global_stop\":%s",
        s->transfer_ready ? "true" : "false", s->flush_requested ? "true" : "false",
        s->local_stop ? "true" : "false", s->global_stop ? "true" : "false");
    json_append(&w,
                ",\"stopped\":%s,\"permit_valid\":%s,\"epoch\":%llu,\"state_version\":%llu,"
                "\"machine_sequence\":%llu,\"sim_time_h\":%.17g}",
                bf_permitted(m, mono) ? "false" : "true", mono < m->permit_until ? "true" : "false",
                (unsigned long long)s->epoch, (unsigned long long)s->state_version,
                (unsigned long long)s->machine_sequence, s->sim_time_h);
    return w.failed ? -1 : 0;
}

/** Start an empty process lifetime; no operation, latch or message is recovered. */
int bf_init(BFMachine *m, const char *id, const char *type, double scale, double flush_h,
            double mono, double wall) {
    memset(m, 0, sizeof *m);
    if (!bf_valid_type(type) || !*id || strlen(id) >= BF_ID || !isfinite(scale) || scale <= 0 ||
        !isfinite(flush_h) || flush_h <= 0) {
        copy_text(m->error, sizeof m->error, "invalid equipment configuration");
        return -1;
    }
    copy_text(m->s.machine_id, sizeof m->s.machine_id, id);
    copy_text(m->s.type, sizeof m->s.type, type);
    copy_text(m->s.state, sizeof m->s.state, "EMPTY");
    bf_uuid(m->s.boot_id, sizeof m->s.boot_id);
    m->s.time_scale = scale;
    m->s.last_wall = wall;
    m->last_mono = mono;
    m->default_flush_h = flush_h;
    return 0;
}

/** Free process-local messages; stopping the process intentionally loses pending work. */
void bf_close(BFMachine *m) {
    while (m->pending_events) {
        BFPendingEvent *event = m->pending_events;
        m->pending_events = event->next;
        free(event->payload);
        free(event);
    }
    while (m->command_results) {
        BFCommandResult *result = m->command_results;
        m->command_results = result->next;
        free(result->fingerprint);
        free(result->response);
        free(result);
    }
}

/** Retain one copy of an event in sequence order until its application acknowledgement. */
static int queue_event(BFMachine *m, const char *id, uint64_t sequence, const char *payload) {
    BFPendingEvent **position = &m->pending_events;
    while (*position && (*position)->sequence < sequence)
        position = &(*position)->next;
    if (*position && !strcmp((*position)->event_id, id))
        return 0;
    BFPendingEvent *event = calloc(1, sizeof *event);
    if (!event)
        return -1;
    event->payload = strdup(payload);
    if (!event->payload) {
        free(event);
        return -1;
    }
    copy_text(event->event_id, sizeof event->event_id, id);
    event->sequence = sequence;
    event->next = *position;
    *position = event;
    return 0;
}

/** Publish a technical transition with actual actuator-command correlation, even after emptying. */
static int emit_event(BFMachine *m, const char *kind, const BFState *affected,
                      const char *command_id, const char *reason, const char *scope,
                      int contents_discarded, double mono, char *response, size_t capacity) {
    char snapshot[BF_JSON], payload[BF_JSON], event_id[BF_ID], utc[40];
    m->s.machine_sequence++;
    m->s.state_version++;
    if (bf_snapshot(m, mono, snapshot, sizeof snapshot))
        return -1;
    bf_uuid(event_id, sizeof event_id);
    bf_utc(m->s.last_wall, utc, sizeof utc);
    Writer w = {payload, sizeof payload, 0, 0};
    json_append(&w, "{\"schema_version\":2,\"event_id\":");
    json_write_string(&w, event_id);
    json_optional_field(&w, "message_id", event_id);
    json_optional_field(&w, "event", kind);
    json_optional_field(&w, "machine_id", m->s.machine_id);
    json_optional_field(&w, "type", m->s.type);
    json_optional_field(&w, "controller_boot_id", m->s.boot_id);
    json_optional_field(&w, "command_id", command_id ? command_id : "");
    json_optional_field(&w, "current_command_id", affected->current_command_id);
    json_optional_field(&w, "interrupted_command_id", affected->interrupted_command_id);
    json_optional_field(&w, "reason", reason ? reason : "");
    json_optional_field(&w, "scope", scope ? scope : "");
    json_optional_field(&w, "ts_utc", utc);
    json_append(&w,
                ",\"contents_discarded\":%s,\"sim_time_h\":%.17g,\"machine_sequence\":%llu,"
                "\"snapshot\":%s}",
                contents_discarded ? "true" : "false", m->s.sim_time_h,
                (unsigned long long)m->s.machine_sequence, snapshot);
    if (w.failed || queue_event(m, event_id, m->s.machine_sequence, payload))
        return -1;
    if (response)
        copy_text(response, capacity, payload);
    return 0;
}

/** Compare a technical operation state with its protocol spelling. */
static int has_state(const BFMachine *m, const char *state) {
    return !strcmp(m->s.state, state);
}

/** Check whether a pipe reservation still prevents independent removal or processing. */
static int transfer_reserved(const BFMachine *m) {
    return *m->s.transfer_role != 0;
}

/** Empty the equipment without changing its identity, clock, counters or stop latches. */
static void empty_equipment(BFState *s) {
    s->occupied = 0;
    s->duration_h = s->elapsed_h = s->target_temp_c = 0;
    s->transfer_ready = s->flush_requested = 0;
    *s->transfer_role = *s->current_command_id = *s->interrupted_command_id = *s->flush_command_id =
        0;
    copy_text(s->state, sizeof s->state, "EMPTY");
}

/** Remember one scalar actuator operation; the scheduler decides what the operation means. */
static void begin_processing(BFState *s, const BFCommand *c) {
    s->occupied = 1;
    s->duration_h = c->duration_h;
    s->elapsed_h = 0;
    s->target_temp_c = c->target_temp_c;
    copy_text(s->current_command_id, sizeof s->current_command_id, c->command_id);
    *s->interrupted_command_id = 0;
    copy_text(s->state, sizeof s->state, "PROCESSING");
}

/** Validate setpoints for exactly one operation, without interpreting production meaning. */
static int valid_setpoints(const BFCommand *c) {
    return c->has_duration && c->has_temperature && isfinite(c->duration_h) && c->duration_h > 0 &&
           isfinite(c->target_temp_c);
}

/** Set or reset exactly one STOP latch; a global reset never clears a local stop. */
static const char *handle_stop_latch(BFMachine *m, const BFCommand *c, int stopped) {
    if (!strcmp(c->scope, "local"))
        m->s.local_stop = stopped;
    else if (!strcmp(c->scope, "global"))
        m->s.global_stop = stopped;
    else
        return "invalid_stop_scope";
    return NULL;
}

/** Load only empty equipment; PROCESS instead requires already occupied, unreserved READY
 * equipment. */
static const char *handle_processing(BFMachine *m, const BFCommand *c, double mono, int loading) {
    if (loading && (!has_state(m, "EMPTY") || m->s.occupied))
        return "machine_not_empty";
    if (!loading && (!has_state(m, "READY") || !m->s.occupied || transfer_reserved(m)))
        return "machine_not_ready";
    if (!bf_permitted(m, mono))
        return "machine_stopped";
    if (!valid_setpoints(c))
        return "invalid_setpoints";
    begin_processing(&m->s, c);
    return NULL;
}

/** Reserve the pipe's source or receiver, without tracking what flows through it. */
static const char *handle_prepare(BFMachine *m, const BFCommand *c, double mono) {
    if (transfer_reserved(m))
        return "transfer_already_reserved";
    if (!bf_permitted(m, mono))
        return "machine_stopped";
    if (!strcmp(c->role, "source")) {
        if (!has_state(m, "READY") || !m->s.occupied)
            return "source_not_ready";
    } else if (!strcmp(c->role, "target")) {
        if (!has_state(m, "EMPTY") || m->s.occupied)
            return "target_not_empty";
        copy_text(m->s.state, sizeof m->s.state, "RESERVED_IN");
    } else
        return "invalid_transfer_role";
    copy_text(m->s.transfer_role, sizeof m->s.transfer_role, c->role);
    m->s.transfer_ready = 0;
    copy_text(m->s.current_command_id, sizeof m->s.current_command_id, c->command_id);
    return NULL;
}

/** Begin timed pipe actuation; timer completion alone never releases an endpoint. */
static const char *handle_execute(BFMachine *m, const BFCommand *c, double mono) {
    if (!transfer_reserved(m))
        return "no_transfer_reservation";
    if (!has_state(m, "READY") && !has_state(m, "RESERVED_IN"))
        return "transfer_already_executing";
    if (!bf_permitted(m, mono) || m->s.flush_requested)
        return "machine_stopped_or_flushing";
    if (!c->has_duration || !isfinite(c->duration_h) || c->duration_h <= 0)
        return "invalid_duration";
    m->s.duration_h = c->duration_h;
    m->s.elapsed_h = 0;
    copy_text(m->s.current_command_id, sizeof m->s.current_command_id, c->command_id);
    copy_text(m->s.state, sizeof m->s.state,
              !strcmp(m->s.transfer_role, "source") ? "TRANSFERRING_OUT" : "TRANSFERRING_IN");
    return NULL;
}

/** Finish a pending flush's setup after pipe reconciliation, preserving its physical command ID. */
static void prepare_flush(BFState *s) {
    copy_text(s->state, sizeof s->state, "FLUSH_PENDING");
    copy_text(s->current_command_id, sizeof s->current_command_id, s->flush_command_id);
    s->duration_h = s->flush_duration_h;
    s->elapsed_h = 0;
}

/** Close a finished transfer: source empties, receiver becomes READY without starting work. */
static const char *handle_commit(BFMachine *m) {
    if (!transfer_reserved(m) || !m->s.transfer_ready)
        return "transfer_not_ready";
    if (!strcmp(m->s.transfer_role, "source")) {
        empty_equipment(&m->s);
    } else {
        *m->s.transfer_role = 0;
        m->s.transfer_ready = 0;
        m->s.occupied = 1;
        m->s.duration_h = m->s.elapsed_h = 0;
        if (m->s.flush_requested)
            prepare_flush(&m->s);
        else
            copy_text(m->s.state, sizeof m->s.state, "READY");
    }
    return NULL;
}

/** Cancel a pipe reservation; version fencing prevents late requests targeting a newer operation.
 */
static const char *handle_abort(BFMachine *m) {
    if (!transfer_reserved(m))
        return NULL;
    if (!strcmp(m->s.transfer_role, "target")) {
        empty_equipment(&m->s);
    } else {
        *m->s.transfer_role = 0;
        m->s.transfer_ready = 0;
        m->s.duration_h = m->s.elapsed_h = 0;
        if (m->s.flush_requested)
            prepare_flush(&m->s);
        else
            copy_text(m->s.state, sizeof m->s.state, "READY");
    }
    return NULL;
}

/** Request removal of current contents; repeated requests do not reset the removal timer. */
static const char *handle_flush(BFMachine *m, const BFCommand *c, const char **event) {
    if (!m->s.occupied && !transfer_reserved(m))
        return "machine_empty";
    if (m->s.flush_requested) {
        *event = "SNAPSHOT";
        return NULL;
    }
    double duration = c->has_duration ? c->duration_h : m->default_flush_h;
    if (!isfinite(duration) || duration <= 0)
        return "invalid_duration";
    m->s.flush_requested = 1;
    m->s.flush_duration_h = duration;
    copy_text(m->s.interrupted_command_id, sizeof m->s.interrupted_command_id,
              m->s.current_command_id);
    copy_text(m->s.flush_command_id, sizeof m->s.flush_command_id, c->command_id);
    if (!transfer_reserved(m))
        prepare_flush(&m->s);
    return NULL;
}

/** Apply the scheduler's explicit output release only to occupied, unreserved READY equipment. */
static const char *handle_release(BFMachine *m, double mono) {
    if (!has_state(m, "READY") || !m->s.occupied || transfer_reserved(m))
        return "machine_not_ready";
    if (!bf_permitted(m, mono))
        return "machine_stopped";
    empty_equipment(&m->s);
    return NULL;
}

/** Select a technical handler; handlers return a reason on rejection and NULL on success. */
static const char *dispatch_command(BFMachine *m, const BFCommand *c, double mono,
                                    const char **event) {
    if (!strcmp(c->cmd, "STOP")) {
        *event = "STOPPED";
        return handle_stop_latch(m, c, 1);
    }
    if (!strcmp(c->cmd, "RESUME")) {
        *event = "RESUMED";
        return handle_stop_latch(m, c, 0);
    }
    if (!strcmp(c->cmd, "SNAPSHOT")) {
        *event = "SNAPSHOT";
        return NULL;
    }
    if (!strcmp(c->cmd, "START") || !strcmp(c->cmd, "PROCESS")) {
        *event = "PROCESS_STARTED";
        return handle_processing(m, c, mono, !strcmp(c->cmd, "START"));
    }
    if (!strcmp(c->cmd, "PREPARE_TRANSFER")) {
        *event = "TRANSFER_PREPARED";
        return handle_prepare(m, c, mono);
    }
    if (!strcmp(c->cmd, "EXECUTE_TRANSFER")) {
        *event = "SNAPSHOT";
        return handle_execute(m, c, mono);
    }
    if (!strcmp(c->cmd, "COMMIT_TRANSFER")) {
        *event = "TRANSFER_COMMITTED";
        return handle_commit(m);
    }
    if (!strcmp(c->cmd, "ABORT_TRANSFER")) {
        *event = "TRANSFER_ABORTED";
        return handle_abort(m);
    }
    if (!strcmp(c->cmd, "FLUSH")) {
        *event = "FLUSH_REQUESTED";
        return handle_flush(m, c, event);
    }
    if (!strcmp(c->cmd, "RELEASE")) {
        *event = "RELEASED";
        return handle_release(m, mono);
    }
    return "unknown_command";
}

/** Encode command semantics without the coordinator epoch, which may change during retry. */
static void command_fingerprint(const BFCommand *c, char *out, size_t capacity) {
    Writer w = {out, capacity, 0, 0};
    json_append(&w, "[");
    json_write_string(&w, c->cmd);
    json_append(&w, ",");
    json_write_string(&w, c->machine_id);
    json_append(&w, ",");
    json_write_string(&w, c->controller_boot_id);
    json_append(&w, ",");
    json_write_string(&w, c->role);
    json_append(&w, ",");
    json_write_string(&w, c->scope);
    json_append(&w, ",%d,%llu,%d,%d,%.17g,%.17g,%d]", c->has_expected_version,
                (unsigned long long)c->expected_version, c->has_duration, c->has_temperature,
                c->duration_h, c->target_temp_c, c->local);
}

/** Find a previously handled command during this controller boot. */
static BFCommandResult *find_result(BFMachine *m, const char *id) {
    for (BFCommandResult *result = m->command_results; result; result = result->next)
        if (!strcmp(result->command_id, id))
            return result;
    return NULL;
}

/** Cache the original event in RAM so a retry does not repeat actuator movement. */
static int cache_result(BFMachine *m, const BFCommand *c, const char *fp, const char *response) {
    BFCommandResult *result = calloc(1, sizeof *result);
    if (!result)
        return -1;
    result->fingerprint = strdup(fp);
    result->response = strdup(response);
    if (!result->fingerprint || !result->response) {
        free(result->fingerprint);
        free(result->response);
        free(result);
        return -1;
    }
    copy_text(result->command_id, sizeof result->command_id, c->command_id);
    result->sequence = m->s.machine_sequence;
    for (BFPendingEvent *event = m->pending_events; event; event = event->next)
        if (event->sequence == result->sequence)
            copy_text(result->event_id, sizeof result->event_id, event->event_id);
    result->next = m->command_results;
    m->command_results = result;
    return 0;
}

/** Renew the volatile lease without clearing STOP; duplicate references cannot extend it. */
static int handle_permit(BFMachine *m, const BFCommand *c, double mono, char *response,
                         size_t capacity) {
    if (c->local || c->epoch < m->s.epoch || !isfinite(c->permit_ms) || c->permit_ms <= 0 ||
        c->permit_ms > 10000 || !isfinite(c->time_scale) || c->time_scale <= 0 ||
        !isfinite(c->sim_time_h) || c->sim_time_h < 0) {
        copy_text(response, capacity, "{\"error\":\"invalid_permit\"}");
        return 1;
    }
    if (!strcmp(c->command_id, m->s.last_permit_id))
        return bf_snapshot(m, mono, response, capacity);
    if (c->epoch == m->s.epoch && *m->s.last_permit_id &&
        c->sim_time_h <= m->s.last_permit_sim_time_h) {
        copy_text(response, capacity, "{\"error\":\"stale_permit\"}");
        return 1;
    }
    m->s.epoch = c->epoch;
    m->s.time_scale = c->time_scale;
    m->s.sim_time_h = fmax(m->s.sim_time_h, c->sim_time_h);
    m->s.last_permit_sim_time_h = c->sim_time_h;
    copy_text(m->s.last_permit_id, sizeof m->s.last_permit_id, c->command_id);
    m->permit_until = mono + c->permit_ms / 1000.;
    return bf_snapshot(m, mono, response, capacity);
}

/** Apply one request; replay precedes version guards, but all new network actions are fenced. */
int bf_command(BFMachine *m, const BFCommand *c, double mono, double wall, char *response,
               size_t capacity) {
    if (!response || capacity < BF_JSON)
        return -1;
    if (bf_tick(m, mono, wall))
        return -1;
    if (!*c->command_id || !*c->cmd || strcmp(c->machine_id, m->s.machine_id)) {
        copy_text(response, capacity, "{\"error\":\"invalid_command_identity\"}");
        return 1;
    }
    int boot_mismatch = !c->local && strcmp(c->cmd, "STOP") && *c->controller_boot_id &&
                        strcmp(c->controller_boot_id, m->s.boot_id);
    if (!strcmp(c->cmd, "PERMIT")) {
        if (boot_mismatch) {
            copy_text(response, capacity, "{\"error\":\"controller_boot_mismatch\"}");
            return 1;
        }
        return handle_permit(m, c, mono, response, capacity);
    }
    char fp[4096];
    command_fingerprint(c, fp, sizeof fp);
    BFCommandResult *previous = find_result(m, c->command_id);
    if (previous) {
        if (strcmp(fp, previous->fingerprint)) {
            copy_text(response, capacity, "{\"error\":\"command_id_conflict\"}");
            return 1;
        }
        copy_text(response, capacity, previous->response);
        return queue_event(m, previous->event_id, previous->sequence, previous->response);
    }
    BFState before = m->s;
    const char *event = "REJECTED", *reason = NULL;
    int bypass = !strcmp(c->cmd, "STOP") || !strcmp(c->cmd, "SNAPSHOT");
    int latch = !strcmp(c->cmd, "RESUME");
    if (boot_mismatch)
        reason = "controller_boot_mismatch";
    else if (!c->local && !bypass && c->epoch != m->s.epoch)
        reason = "stale_coordinator_epoch";
    else if (!c->local && !bypass && !latch && !c->has_expected_version)
        reason = "expected_state_version_required";
    else if (!bypass && c->has_expected_version && c->expected_version != m->s.state_version)
        reason = "state_version_conflict";
    else
        reason = dispatch_command(m, c, mono, &event);
    if (reason) {
        m->s = before;
        event = "REJECTED";
    }
    BFState affected = m->s;
    int releasing = !strcmp(event, "TRANSFER_COMMITTED") || !strcmp(event, "TRANSFER_ABORTED") ||
                    !strcmp(event, "RELEASED");
    if (releasing)
        affected = before;
    int discarded = releasing && before.flush_requested;
    if (emit_event(m, event, &affected, c->command_id, reason, c->scope, discarded, mono, response,
                   capacity) ||
        cache_result(m, c, fp, response)) {
        copy_text(m->error, sizeof m->error, "event/result allocation failed");
        return -1;
    }
    return reason ? 1 : 0;
}

/** Advance only equipment clocks; production policy belongs to the scheduler.
 */
int bf_tick(BFMachine *m, double mono, double wall) {
    if (!isfinite(mono) || mono < m->last_mono || !isfinite(wall))
        return -1;
    double old_mono = m->last_mono;
    m->s.sim_time_h += (mono - old_mono) / m->s.time_scale;
    m->s.last_wall = wall;
    m->last_mono = mono;
    double enabled_h = 0;
    if (!m->s.local_stop && !m->s.global_stop)
        enabled_h = fmax(0, fmin(mono, m->permit_until) - old_mono) / m->s.time_scale;
    if (enabled_h <= 0)
        return 0;
    if (has_state(m, "FLUSH_PENDING") && !transfer_reserved(m)) {
        copy_text(m->s.state, sizeof m->s.state, "FLUSHING");
        m->s.state_version++;
    }
    int processing = has_state(m, "PROCESSING");
    int transfer = has_state(m, "TRANSFERRING_IN") || has_state(m, "TRANSFERRING_OUT");
    int flushing = has_state(m, "FLUSHING");
    if ((!processing && !transfer && !flushing) ||
        (transfer && (m->s.transfer_ready || m->s.flush_requested)))
        return 0;
    m->s.elapsed_h = fmin(m->s.duration_h, m->s.elapsed_h + enabled_h);
    if (m->s.elapsed_h + 1e-9 < m->s.duration_h)
        return 0;
    BFState affected = m->s;
    const char *event;
    if (processing) {
        copy_text(m->s.state, sizeof m->s.state, "READY");
        event = "PROCESS_COMPLETED";
    } else if (transfer) {
        m->s.transfer_ready = 1;
        event = "TRANSFER_READY";
    } else {
        empty_equipment(&m->s);
        event = "FLUSHED";
    }
    return emit_event(m, event, &affected, affected.current_command_id, NULL, NULL, flushing, mono,
                      NULL, 0);
}

/** Remove exactly one acknowledged RAM event; duplicate ACKs require no special handling. */
int bf_ack(BFMachine *m, const char *event_id) {
    BFPendingEvent **position = &m->pending_events;
    while (*position) {
        BFPendingEvent *event = *position;
        if (!strcmp(event->event_id, event_id)) {
            *position = event->next;
            free(event->payload);
            free(event);
            break;
        }
        position = &event->next;
    }
    return 0;
}

/** Retry pending events in creation order; successful enqueue is not an application ACK. */
int bf_outbox(BFMachine *m, int limit, int (*send)(const char *, void *), void *context) {
    int count = 0;
    for (BFPendingEvent *event = m->pending_events; event && count < limit; event = event->next) {
        if (send(event->payload, context))
            break;
        count++;
    }
    return count;
}

/** Publish unsent events in order; reset this cursor on reconnect, while RAM history still exists.
 */
int bf_outbox_new(BFMachine *m, uint64_t *after_sequence, int limit,
                  int (*send)(const char *, void *), void *context) {
    int count = 0;
    for (BFPendingEvent *event = m->pending_events; event && count < limit; event = event->next) {
        if (event->sequence <= *after_sequence)
            continue;
        if (send(event->payload, context))
            break;
        *after_sequence = event->sequence;
        count++;
    }
    return count;
}
