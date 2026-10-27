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

/* State, command results and unacknowledged events commit together. Networking never
   owns the machine state; only the main controller thread enters this module. */

typedef struct { char *buf; size_t cap, n; int failed; } Writer;
static void put(Writer *w, const char *format, ...) {
    if (w->failed) return;
    va_list args; va_start(args, format);
    int n = vsnprintf(w->buf + w->n, w->cap - w->n, format, args);
    va_end(args);
    if (n < 0 || (size_t)n >= w->cap - w->n) w->failed = 1;
    else w->n += (size_t)n;
}
static void str(Writer *w, const char *s) {
    put(w, "\"");
    for (const unsigned char *p = (const unsigned char *)s; *p; p++) {
        if (*p == '"' || *p == '\\') put(w, "\\%c", *p);
        else if (*p < 32) put(w, "\\u%04x", *p);
        else put(w, "%c", *p);
    }
    put(w, "\"");
}
int bf_json_string(const char *value, char *out, size_t cap) {
    Writer w = {out, cap, 0, 0}; str(&w, value); return w.failed ? -1 : 0;
}
static void field(Writer *w, const char *key, const char *value) {
    put(w, ",\"%s\":", key); if (*value) str(w, value); else put(w, "null");
}
static void copy(char *to, size_t n, const char *from) { snprintf(to, n, "%s", from ? from : ""); }
void bf_utc(double wall, char *out, size_t cap) {
    time_t sec = (time_t)wall; struct tm t; gmtime_r(&sec, &t);
    strftime(out, cap, "%Y-%m-%dT%H:%M:%SZ", &t);
}
void bf_uuid(char *out, size_t cap) {
    unsigned char bytes[16]; int fd = open("/dev/urandom", O_RDONLY);
    ssize_t n = fd >= 0 ? read(fd, bytes, sizeof bytes) : -1;
    if (fd >= 0) close(fd);
    if (n != (ssize_t)sizeof bytes) abort();
    bytes[6] = (unsigned char)((bytes[6] & 15) | 64);
    bytes[8] = (unsigned char)((bytes[8] & 63) | 128);
    snprintf(out, cap, "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
             bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
             bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]);
}
int bf_valid_type(const char *type) {
    const char *types[] = {"mash", "lauter", "kettle", "whirlpool", "unitank", "filter"};
    for (size_t i = 0; i < sizeof types / sizeof *types; i++) if (!strcmp(types[i], type)) return 1;
    return 0;
}
int bf_permitted(const BFMachine *m, double mono) {
    return mono < m->permit_until && !m->s.local_stop && !m->s.global_stop;
}
int bf_snapshot(const BFMachine *m, double mono, char *out, size_t cap) {
    const BFState *s = &m->s; Writer w = {out, cap, 0, 0}; char utc[40]; bf_utc(s->last_wall, utc, sizeof utc);
    put(&w, "{\"schema_version\":1,\"machine_id\":"); str(&w, s->machine_id);
    field(&w, "type", s->type); field(&w, "state", s->state); field(&w, "batch_id", s->batch_id);
    field(&w, "execution_id", s->execution_id); field(&w, "transfer_id", s->transfer_id);
    field(&w, "recipe_version_id", s->recipe_version_id); field(&w, "run_id", s->run_id);
    field(&w, "controller_boot_id", s->boot_id); field(&w, "ts_utc", utc);
    field(&w, "phase", s->phase_index < s->phase_count ? s->phases[s->phase_index].name : "");
    double duration = s->phase_index < s->phase_count ? s->phases[s->phase_index].duration_h : 0;
    put(&w, ",\"phase_index\":%d,\"phase_elapsed_h\":%.17g,\"phase_duration_h\":%.17g",
        s->phase_index, s->phase_elapsed_h, duration);
    put(&w, ",\"hold_deadline_h\":"); if (s->hold_deadline_h >= 0) put(&w, "%.17g", s->hold_deadline_h); else put(&w, "null");
    put(&w, ",\"local_stop\":%s,\"global_stop\":%s,\"stopped\":%s,\"permit_valid\":%s",
        s->local_stop ? "true" : "false", s->global_stop ? "true" : "false",
        bf_permitted(m, mono) ? "false" : "true", mono < m->permit_until ? "true" : "false");
    put(&w, ",\"epoch\":%llu,\"state_version\":%llu,\"machine_sequence\":%llu,\"sim_time_h\":%.17g",
        (unsigned long long)s->epoch, (unsigned long long)s->state_version,
        (unsigned long long)s->machine_sequence, s->sim_time_h);
    field(&w, "transfer_role", s->transfer_role); field(&w, "peer_id", s->peer_id);
    field(&w, "reason", s->reason); field(&w, "last_permit_id", s->last_permit_id);
    put(&w, ",\"last_permit_sim_time_h\":%.17g", s->last_permit_sim_time_h);
    put(&w, ",\"transfer_elapsed_h\":%.17g,\"transfer_duration_h\":%.17g,\"transfer_ready\":%s",
        s->transfer_elapsed_h, s->transfer_duration_h, s->transfer_ready ? "true" : "false");
    put(&w, ",\"hold_h\":%.17g,\"max_pause_h\":%.17g,\"pause_elapsed_h\":%.17g,\"flush_h\":%.17g,\"flush_elapsed_h\":%.17g",
        s->hold_h, s->max_pause_h, s->pause_elapsed_h, s->flush_h, s->flush_elapsed_h);
    put(&w, ",\"flush_requested\":%s,\"spoiled\":%s,\"time_scale\":%.17g,\"last_wall\":%.17g,\"phases\":[",
        s->flush_requested ? "true" : "false", s->spoiled ? "true" : "false", s->time_scale, s->last_wall);
    for (int i = 0; i < s->phase_count; i++) {
        if (i) put(&w, ",");
        put(&w, "{\"name\":"); str(&w, s->phases[i].name);
        put(&w, ",\"duration_h\":%.17g,\"target_temp_c\":%.17g}", s->phases[i].duration_h, s->phases[i].target_temp_c);
    }
    put(&w, "]}"); return w.failed ? -1 : 0;
}
static int sql(BFMachine *m, const char *query) {
    char *err = NULL; int rc = sqlite3_exec(m->db, query, NULL, NULL, &err);
    if (rc != SQLITE_OK) { copy(m->error, sizeof m->error, err); sqlite3_free(err); return -1; }
    return 0;
}
static int prepare(BFMachine *m, sqlite3_stmt **stmt, const char *query) {
    if (sqlite3_prepare_v2(m->db, query, -1, stmt, NULL) != SQLITE_OK) {
        copy(m->error, sizeof m->error, sqlite3_errmsg(m->db)); return -1;
    }
    return 0;
}
static int done(BFMachine *m, sqlite3_stmt *stmt) {
    int rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) copy(m->error, sizeof m->error, sqlite3_errmsg(m->db));
    sqlite3_finalize(stmt); return rc == SQLITE_DONE ? 0 : -1;
}
static int save(BFMachine *m, double mono) {
    char payload[BF_JSON]; sqlite3_stmt *q;
    if (bf_snapshot(m, mono, payload, sizeof payload) || prepare(m, &q, "INSERT INTO machine_state(id,payload) VALUES(1,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload")) return -1;
    sqlite3_bind_text(q, 1, payload, -1, SQLITE_TRANSIENT); return done(m, q);
}
static int load_text(BFMachine *m, const char *path, char *out, size_t n) {
    sqlite3_stmt *q; if (prepare(m, &q, "SELECT json_extract(payload,?) FROM machine_state WHERE id=1")) return -1;
    sqlite3_bind_text(q, 1, path, -1, SQLITE_STATIC);
    int rc = sqlite3_step(q); if (rc == SQLITE_ROW) copy(out, n, (const char *)sqlite3_column_text(q, 0));
    sqlite3_finalize(q); return rc == SQLITE_ROW ? 0 : -1;
}
static double load_num(BFMachine *m, const char *path, double fallback) {
    sqlite3_stmt *q; if (prepare(m, &q, "SELECT json_extract(payload,?) FROM machine_state WHERE id=1")) return fallback;
    sqlite3_bind_text(q, 1, path, -1, SQLITE_STATIC);
    double v = fallback;
    if (sqlite3_step(q) == SQLITE_ROW && sqlite3_column_type(q, 0) != SQLITE_NULL) v = sqlite3_column_double(q, 0);
    sqlite3_finalize(q); return v;
}
static int load(BFMachine *m) {
    char id[BF_ID] = "", type[BF_NAME] = "";
    if (load_text(m, "$.machine_id", id, sizeof id)) return 0;
    load_text(m, "$.type", type, sizeof type);
    if (strcmp(id, m->s.machine_id) || strcmp(type, m->s.type) || load_num(m, "$.schema_version", 0) != 1) {
        copy(m->error, sizeof m->error, "journal identity or schema mismatch"); return -1;
    }
#define TXT(name) load_text(m, "$." #name, m->s.name, sizeof m->s.name)
#define NUM(name) m->s.name = load_num(m, "$." #name, m->s.name)
    TXT(state); TXT(batch_id); TXT(execution_id); TXT(recipe_version_id); TXT(run_id);
    TXT(transfer_id); TXT(transfer_role); TXT(peer_id); TXT(last_permit_id); TXT(reason);
    NUM(phase_index); NUM(phase_elapsed_h); NUM(transfer_elapsed_h); NUM(transfer_duration_h);
    NUM(hold_h); NUM(hold_deadline_h); NUM(max_pause_h); NUM(pause_elapsed_h); NUM(flush_h);
    NUM(flush_elapsed_h); NUM(sim_time_h); NUM(time_scale); NUM(last_wall); NUM(local_stop);
    NUM(global_stop); NUM(transfer_ready); NUM(flush_requested); NUM(spoiled); NUM(epoch);
    NUM(state_version); NUM(machine_sequence); NUM(last_permit_sim_time_h);
#undef TXT
#undef NUM
    for (int i = 0; i < BF_PHASES; i++) {
        char path[80]; snprintf(path, sizeof path, "$.phases[%d].name", i);
        load_text(m, path, m->s.phases[i].name, sizeof m->s.phases[i].name);
        if (!*m->s.phases[i].name) break;
        snprintf(path, sizeof path, "$.phases[%d].duration_h", i); m->s.phases[i].duration_h = load_num(m, path, 0);
        snprintf(path, sizeof path, "$.phases[%d].target_temp_c", i); m->s.phases[i].target_temp_c = load_num(m, path, 0);
        m->s.phase_count++;
    }
    return 1;
}
int bf_open(BFMachine *m, const char *path, const char *id, const char *type,
            double scale, double flush_h, double mono, double wall) {
    memset(m, 0, sizeof *m);
    if (!bf_valid_type(type) || !*id || strlen(id) >= BF_ID || !isfinite(scale) || scale <= 0 || !isfinite(flush_h) || flush_h < 0) {
        copy(m->error, sizeof m->error, "invalid machine configuration"); return -1;
    }
    copy(m->s.machine_id, sizeof m->s.machine_id, id); copy(m->s.type, sizeof m->s.type, type);
    copy(m->s.state, sizeof m->s.state, "EMPTY"); m->s.hold_deadline_h = -1;
    m->s.time_scale = scale; m->s.flush_h = flush_h; m->s.last_wall = wall;
    m->last_mono = mono; m->last_checkpoint = mono;
    if (sqlite3_open(path, &m->db) != SQLITE_OK) { copy(m->error, sizeof m->error, sqlite3_errmsg(m->db)); return -1; }
    sqlite3_busy_timeout(m->db, 1000);
    if (sql(m, "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; CREATE TABLE IF NOT EXISTS machine_state(id INTEGER PRIMARY KEY CHECK(id=1),payload TEXT NOT NULL CHECK(json_valid(payload))); CREATE TABLE IF NOT EXISTS transfer_decisions(id TEXT PRIMARY KEY,decision TEXT NOT NULL); CREATE TABLE IF NOT EXISTS commands(id TEXT PRIMARY KEY,fingerprint TEXT NOT NULL,result TEXT NOT NULL); CREATE TABLE IF NOT EXISTS outbox(seq INTEGER PRIMARY KEY AUTOINCREMENT,event_id TEXT NOT NULL UNIQUE,payload TEXT NOT NULL);")) return -1;
    int existing = load(m); if (existing < 0) return -1;
    if (existing) {
        double offline_h = fmax(0, wall - m->s.last_wall) / m->s.time_scale;
        m->s.sim_time_h += offline_h;
        if (!strcmp(m->s.state, "PROCESSING")) m->s.pause_elapsed_h += offline_h;
    }
    bf_uuid(m->s.boot_id, sizeof m->s.boot_id); m->s.last_wall = wall;
    return save(m, mono);
}
int bf_checkpoint(BFMachine *m, double mono) { return save(m, mono); }
void bf_close(BFMachine *m) { if (m->db) sqlite3_close(m->db); m->db = NULL; }
static int emit(BFMachine *m, const char *event, const BFState *affected, const char *cmd_id,
                const char *reason, double mono, char *result, size_t cap, const char *scope) {
    char snapshot[BF_JSON], event_id[BF_ID], utc[40], payload[BF_JSON];
    m->s.machine_sequence++; m->s.state_version++;
    if (bf_snapshot(m, mono, snapshot, sizeof snapshot)) return -1;
    bf_uuid(event_id, sizeof event_id); bf_utc(m->s.last_wall, utc, sizeof utc);
    Writer w = {payload, sizeof payload, 0, 0}; put(&w, "{\"schema_version\":1,\"event_id\":"); str(&w, event_id);
    field(&w, "message_id", event_id); field(&w, "event", event); field(&w, "machine_id", m->s.machine_id);
    field(&w, "type", m->s.type); field(&w, "controller_boot_id", m->s.boot_id);
    field(&w, "batch_id", affected->batch_id); field(&w, "execution_id", affected->execution_id);
    field(&w, "transfer_id", affected->transfer_id); field(&w, "recipe_version_id", affected->recipe_version_id);
    field(&w, "run_id", affected->run_id); field(&w, "command_id", cmd_id ? cmd_id : "");
    field(&w, "scope", scope ? scope : ""); field(&w, "reason", reason ? reason : ""); field(&w, "ts_utc", utc);
    field(&w, "discard_reason", affected->spoiled || affected->flush_requested ? affected->reason : "");
    put(&w, ",\"sim_time_h\":%.17g,\"machine_sequence\":%llu,\"snapshot\":%s}", m->s.sim_time_h,
        (unsigned long long)m->s.machine_sequence, snapshot);
    if (w.failed) return -1;
    sqlite3_stmt *q; if (prepare(m, &q, "INSERT INTO outbox(event_id,payload) VALUES(?,?)")) return -1;
    sqlite3_bind_text(q, 1, event_id, -1, SQLITE_TRANSIENT); sqlite3_bind_text(q, 2, payload, -1, SQLITE_TRANSIENT);
    if (done(m, q)) return -1;
    if (result) copy(result, cap, payload);
    return 0;
}
static void empty(BFState *s) {
    *s->batch_id = *s->execution_id = *s->recipe_version_id = *s->run_id = 0;
    *s->transfer_id = *s->transfer_role = *s->peer_id = *s->reason = 0;
    s->phase_count = s->phase_index = s->transfer_ready = s->flush_requested = s->spoiled = 0;
    s->phase_elapsed_h = s->transfer_elapsed_h = s->transfer_duration_h = 0;
    s->hold_h = s->max_pause_h = s->pause_elapsed_h = s->flush_elapsed_h = 0;
    s->hold_deadline_h = -1; memset(s->phases, 0, sizeof s->phases);
    copy(s->state, sizeof s->state, "EMPTY");
}
static int is_state(const BFMachine *m, const char *state) { return !strcmp(m->s.state, state); }
static int active_transfer(const BFMachine *m) { return *m->s.transfer_id != 0; }
static int transfer_decided(BFMachine *m, const char *id) {
    sqlite3_stmt *q; if (prepare(m, &q, "SELECT 1 FROM transfer_decisions WHERE id=?")) return -1;
    sqlite3_bind_text(q, 1, id, -1, SQLITE_STATIC);
    int found = sqlite3_step(q) == SQLITE_ROW; sqlite3_finalize(q); return found;
}
static int record_transfer(BFMachine *m, const char *id, const char *decision) {
    sqlite3_stmt *q; if (prepare(m, &q, "INSERT OR IGNORE INTO transfer_decisions(id,decision) VALUES(?,?)")) return -1;
    sqlite3_bind_text(q, 1, id, -1, SQLITE_STATIC); sqlite3_bind_text(q, 2, decision, -1, SQLITE_STATIC);
    return done(m, q);
}
static int valid_phases(const BFMachine *m, const BFCommand *c) {
    if (c->phase_count < 1 || c->phase_count > BF_PHASES || !*c->batch_id || !*c->execution_id || !*c->recipe_version_id) return 0;
    if (!isfinite(c->hold_h) || c->hold_h < 0 || !isfinite(c->max_pause_h) || c->max_pause_h < 0) return 0;
    if (!strcmp(m->s.type, "unitank") && (c->phase_count != 2 || strcmp(c->phases[0].name, "fermentation") || strcmp(c->phases[1].name, "maturation"))) return 0;
    for (int i = 0; i < c->phase_count; i++)
        if (!*c->phases[i].name || !isfinite(c->phases[i].duration_h) || c->phases[i].duration_h <= 0 || !isfinite(c->phases[i].target_temp_c)) return 0;
    return 1;
}
static void assign(BFState *s, const BFCommand *c) {
    copy(s->batch_id, sizeof s->batch_id, c->batch_id); copy(s->execution_id, sizeof s->execution_id, c->execution_id);
    copy(s->recipe_version_id, sizeof s->recipe_version_id, c->recipe_version_id); copy(s->run_id, sizeof s->run_id, c->run_id);
    s->phase_count = c->phase_count; memcpy(s->phases, c->phases, sizeof s->phases);
    s->phase_index = 0; s->phase_elapsed_h = 0; s->hold_h = c->hold_h; s->max_pause_h = c->max_pause_h;
    s->hold_deadline_h = -1; s->pause_elapsed_h = 0; s->flush_requested = s->spoiled = 0;
}
/* Store the canonical semantic payload itself, avoiding hash collisions and
   delimiter ambiguity. Epoch is fencing metadata; leader handover may change it. */
static void fingerprint(const BFCommand *c, char *out, size_t cap) {
    Writer w = {out, cap, 0, 0};
    const char *values[] = {c->cmd, c->machine_id, c->batch_id, c->execution_id,
        c->recipe_version_id, c->run_id, c->transfer_id, c->role, c->peer_id,
        c->scope, c->reason, c->discard_reason};
    put(&w, "[");
    for (size_t i = 0; i < sizeof values / sizeof *values; i++) {
        if (i) put(&w, ",");
        str(&w, values[i]);
    }
    put(&w, ",%d,%llu,%.17g,%.17g,%.17g,%d,%d", c->has_expected_version,
        (unsigned long long)c->expected_version, c->duration_h, c->hold_h,
        c->max_pause_h, c->local, c->phase_count);
    for (int i = 0; i < c->phase_count && i < BF_PHASES; i++) {
        put(&w, ",["); str(&w, c->phases[i].name);
        put(&w, ",%.17g,%.17g]", c->phases[i].duration_h, c->phases[i].target_temp_c);
    }
    put(&w, "]");
}
static int cache_result(BFMachine *m, const BFCommand *c, const char *fp, const char *response) {
    sqlite3_stmt *q; if (prepare(m, &q, "INSERT INTO commands(id,fingerprint,result) VALUES(?,?,?)")) return -1;
    sqlite3_bind_text(q, 1, c->command_id, -1, SQLITE_STATIC); sqlite3_bind_text(q, 2, fp, -1, SQLITE_STATIC);
    sqlite3_bind_text(q, 3, response, -1, SQLITE_STATIC); return done(m, q);
}
int bf_command(BFMachine *m, const BFCommand *c, double mono, double wall, char *response, size_t cap) {
    if (!response || cap < BF_JSON) return -1;
    *response = 0;
    if (bf_tick(m, mono, wall)) return -1;
    if (!*c->command_id || !*c->cmd || strcmp(c->machine_id, m->s.machine_id)) {
        copy(response, cap, "{\"error\":\"invalid_command_identity\"}"); return 1;
    }
    if (!strcmp(c->cmd, "PERMIT")) {
        if (c->local || c->epoch < m->s.epoch || !isfinite(c->permit_ms) || c->permit_ms <= 0 || c->permit_ms > 10000 || !isfinite(c->time_scale) || c->time_scale <= 0 || !isfinite(c->sim_time_h) || c->sim_time_h < 0) {
            copy(response, cap, "{\"error\":\"invalid_permit\"}"); return 1;
        }
        if (!strcmp(c->command_id, m->s.last_permit_id)) return bf_snapshot(m, mono, response, cap);
        if (c->epoch == m->s.epoch && *m->s.last_permit_id && c->sim_time_h <= m->s.last_permit_sim_time_h) {
            copy(response, cap, "{\"error\":\"stale_permit\"}"); return 1;
        }
        m->s.last_permit_sim_time_h = c->sim_time_h;
        m->s.epoch = c->epoch; m->s.time_scale = c->time_scale;
        m->s.sim_time_h = fmax(m->s.sim_time_h, c->sim_time_h);
        m->permit_until = mono + c->permit_ms / 1000.;
        copy(m->s.last_permit_id, sizeof m->s.last_permit_id, c->command_id);
        if (save(m, mono)) return -1;
        return bf_snapshot(m, mono, response, cap);
    }
    char fp[16384]; fingerprint(c, fp, sizeof fp); sqlite3_stmt *q;
    if (prepare(m, &q, "SELECT fingerprint,result FROM commands WHERE id=?")) return -1;
    sqlite3_bind_text(q, 1, c->command_id, -1, SQLITE_STATIC);
    if (sqlite3_step(q) == SQLITE_ROW) {
        int same = !strcmp(fp, (const char *)sqlite3_column_text(q, 0));
        if (same) copy(response, cap, (const char *)sqlite3_column_text(q, 1));
        else copy(response, cap, "{\"error\":\"command_id_conflict\"}");
        sqlite3_finalize(q);
        if (same) {
            if (prepare(m, &q, "INSERT OR IGNORE INTO outbox(event_id,payload) VALUES(json_extract(?, '$.event_id'),?)")) return -1;
            sqlite3_bind_text(q, 1, response, -1, SQLITE_STATIC); sqlite3_bind_text(q, 2, response, -1, SQLITE_STATIC);
            if (done(m, q)) return -1;
        }
        return same ? 0 : 1;
    }
    sqlite3_finalize(q);
    BFState before = m->s;
    if (sql(m, "BEGIN IMMEDIATE")) return -1;
    const char *event = "REJECTED", *reason = NULL;
    if (!c->local && strcmp(c->cmd, "STOP") && strcmp(c->cmd, "SNAPSHOT") && c->epoch != m->s.epoch) reason = "stale_coordinator_epoch";
    else if (c->has_expected_version && c->expected_version != m->s.state_version && strcmp(c->cmd, "STOP") && strcmp(c->cmd, "SNAPSHOT")) reason = "state_version_conflict";
    else if (!strcmp(c->cmd, "STOP")) {
        if (!strcmp(c->scope, "local")) m->s.local_stop = 1;
        else if (!strcmp(c->scope, "global")) m->s.global_stop = 1;
        else reason = "invalid_stop_scope";
        if (!reason) event = "STOPPED";
    } else if (!strcmp(c->cmd, "RESUME")) {
        if (!strcmp(c->scope, "local")) m->s.local_stop = 0;
        else if (!strcmp(c->scope, "global")) m->s.global_stop = 0;
        else reason = "invalid_stop_scope";
        if (!reason) event = "RESUMED";
    } else if (!strcmp(c->cmd, "SNAPSHOT")) event = "SNAPSHOT";
    else if (!strcmp(c->cmd, "START")) {
        if (!is_state(m, "EMPTY")) reason = "machine_not_empty";
        else if (!bf_permitted(m, mono)) reason = "machine_stopped";
        else if (!valid_phases(m, c)) reason = "invalid_stage_definition";
        else { assign(&m->s, c); copy(m->s.state, sizeof m->s.state, "PROCESSING"); event = "STARTED"; }
    } else if (!strcmp(c->cmd, "PREPARE_TRANSFER")) {
        if (!*c->transfer_id || !*c->peer_id || !strcmp(c->peer_id, m->s.machine_id)) reason = "invalid_transfer";
        else if (transfer_decided(m, c->transfer_id)) reason = "transfer_already_decided";
        else if (active_transfer(m)) reason = "transfer_already_reserved";
        else if (!bf_permitted(m, mono)) reason = "machine_stopped";
        else if (!strcmp(c->role, "source")) {
            if (!is_state(m, "READY_TO_TRANSFER") || strcmp(c->batch_id, m->s.batch_id) || m->s.spoiled) reason = "source_not_ready";
        } else if (!strcmp(c->role, "target")) {
            if (!is_state(m, "EMPTY")) reason = "target_not_empty";
            else if (!valid_phases(m, c)) reason = "invalid_stage_definition";
            else { assign(&m->s, c); copy(m->s.state, sizeof m->s.state, "RESERVED_IN"); }
        } else reason = "invalid_transfer_role";
        if (!reason) {
            copy(m->s.transfer_id, sizeof m->s.transfer_id, c->transfer_id); copy(m->s.transfer_role, sizeof m->s.transfer_role, c->role);
            copy(m->s.peer_id, sizeof m->s.peer_id, c->peer_id); m->s.transfer_ready = 0; m->s.transfer_elapsed_h = 0;
            event = "TRANSFER_PREPARED";
        }
    } else if (!strcmp(c->cmd, "EXECUTE_TRANSFER")) {
        if (!active_transfer(m) || strcmp(c->transfer_id, m->s.transfer_id)) reason = "unknown_transfer";
        else if (!is_state(m, "READY_TO_TRANSFER") && !is_state(m, "RESERVED_IN")) reason = "transfer_already_executing";
        else if (!bf_permitted(m, mono) || m->s.flush_requested || m->s.spoiled) reason = "machine_stopped_or_spoiled";
        else if (!isfinite(c->duration_h) || c->duration_h <= 0) reason = "invalid_transfer_duration";
        else {
            m->s.transfer_duration_h = c->duration_h; m->s.transfer_elapsed_h = 0;
            copy(m->s.state, sizeof m->s.state, !strcmp(m->s.transfer_role, "source") ? "TRANSFERRING_OUT" : "TRANSFERRING_IN");
            event = "SNAPSHOT";
        }
    } else if (!strcmp(c->cmd, "COMMIT_TRANSFER")) {
        if (!active_transfer(m) || strcmp(c->transfer_id, m->s.transfer_id) || !m->s.transfer_ready) reason = "transfer_not_ready";
        else {
            if (!strcmp(m->s.transfer_role, "source")) empty(&m->s);
            else {
                *m->s.transfer_id = *m->s.transfer_role = *m->s.peer_id = 0;
                m->s.transfer_ready = 0; m->s.transfer_elapsed_h = m->s.transfer_duration_h = 0;
                if (*c->discard_reason) { m->s.flush_requested = 1; copy(m->s.reason, sizeof m->s.reason, c->discard_reason); }
                copy(m->s.state, sizeof m->s.state, m->s.flush_requested || m->s.spoiled ? "FLUSH_PENDING" : "PROCESSING");
            }
            event = "TRANSFER_COMMITTED";
        }
    } else if (!strcmp(c->cmd, "ABORT_TRANSFER")) {
        if (*c->transfer_id && !active_transfer(m) && is_state(m, "EMPTY") && !transfer_decided(m, c->transfer_id)) event = "TRANSFER_ABORTED";
        else if (!active_transfer(m) || strcmp(c->transfer_id, m->s.transfer_id)) reason = "unknown_transfer";
        else {
            if (!strcmp(m->s.transfer_role, "target")) empty(&m->s);
            else {
                *m->s.transfer_id = *m->s.transfer_role = *m->s.peer_id = 0;
                m->s.transfer_ready = 0; m->s.transfer_elapsed_h = m->s.transfer_duration_h = 0;
                copy(m->s.state, sizeof m->s.state, m->s.flush_requested || m->s.spoiled ? "FLUSH_PENDING" : "READY_TO_TRANSFER");
            }
            event = "TRANSFER_ABORTED";
        }
    } else if (!strcmp(c->cmd, "FLUSH")) {
        if (!*c->batch_id || strcmp(c->batch_id, m->s.batch_id)) reason = "batch_mismatch";
        else {
            m->s.flush_requested = 1; copy(m->s.reason, sizeof m->s.reason, *c->reason ? c->reason : "operator");
            if (!active_transfer(m)) copy(m->s.state, sizeof m->s.state, "FLUSH_PENDING");
            event = "FLUSH_REQUESTED";
        }
    } else reason = "unknown_command";
    if (reason) { m->s = before; event = "REJECTED"; }
    BFState affected = m->s;
    if (!strcmp(event, "TRANSFER_COMMITTED") || !strcmp(event, "TRANSFER_ABORTED")) affected = before;
    if (!strcmp(event, "TRANSFER_ABORTED") && !*affected.transfer_id) {
        copy(affected.transfer_id, sizeof affected.transfer_id, c->transfer_id);
        copy(affected.batch_id, sizeof affected.batch_id, c->batch_id);
    }
    if ((!strcmp(event, "TRANSFER_ABORTED") || !strcmp(event, "TRANSFER_COMMITTED")) &&
        record_transfer(m, c->transfer_id, event)) { sql(m, "ROLLBACK"); m->s = before; return -1; }
    if (!strcmp(event, "TRANSFER_COMMITTED") && *c->discard_reason) {
        affected.flush_requested = 1; copy(affected.reason, sizeof affected.reason, c->discard_reason);
    }
    if (emit(m, event, &affected, c->command_id, reason ? reason : (!strcmp(event, "FLUSH_REQUESTED") ? m->s.reason : NULL), mono, response, cap, c->scope) || save(m, mono) || cache_result(m, c, fp, response) || sql(m, "COMMIT")) {
        sql(m, "ROLLBACK"); m->s = before; return -1;
    }
    return reason ? 1 : 0;
}
static int spoil(BFMachine *m, const char *reason, double mono) {
    if (m->s.spoiled || m->s.flush_requested) return 0;
    m->s.spoiled = m->s.flush_requested = 1; copy(m->s.reason, sizeof m->s.reason, reason);
    if (!active_transfer(m)) copy(m->s.state, sizeof m->s.state, "FLUSH_PENDING");
    return emit(m, "SPOILED", &m->s, NULL, reason, mono, NULL, 0, NULL);
}
int bf_tick(BFMachine *m, double mono, double wall) {
    if (!isfinite(mono) || mono < m->last_mono || !isfinite(wall)) return -1;
    double elapsed = mono - m->last_mono;
    BFState before = m->s; double old_mono = m->last_mono;
    m->last_mono = mono; m->s.last_wall = wall;
    double sim_delta = elapsed / m->s.time_scale; m->s.sim_time_h += sim_delta;
    double enabled_real = 0;
    if (!m->s.local_stop && !m->s.global_stop) enabled_real = fmax(0, fmin(mono, m->permit_until) - old_mono);
    double enabled_h = enabled_real / m->s.time_scale;
    if (sql(m, "BEGIN IMMEDIATE")) { m->s = before; m->last_mono = old_mono; return -1; }
    int failed = 0;
    if (*m->s.batch_id && strcmp(m->s.transfer_role, "target")) {
        if (is_state(m, "PROCESSING")) {
            m->s.pause_elapsed_h += fmax(0, sim_delta - enabled_h);
            if (m->s.max_pause_h > 0 && m->s.pause_elapsed_h > m->s.max_pause_h + 1e-9) failed = spoil(m, "interruption_limit", mono);
        }
        if (!failed && m->s.hold_deadline_h >= 0 && m->s.sim_time_h > m->s.hold_deadline_h + 1e-9)
            failed = spoil(m, "hold_timeout", mono);
    }
    if (!failed && !m->s.spoiled && !m->s.flush_requested && enabled_h > 0 && is_state(m, "PROCESSING")) {
        double remaining = enabled_h;
        while (remaining > 1e-12 && is_state(m, "PROCESSING")) {
            BFPhase *p = &m->s.phases[m->s.phase_index];
            double step = fmin(remaining, fmax(0, p->duration_h - m->s.phase_elapsed_h));
            m->s.phase_elapsed_h += step; remaining -= step;
            if (m->s.phase_elapsed_h + 1e-9 >= p->duration_h) {
                if (emit(m, "PHASE_COMPLETED", &m->s, NULL, NULL, mono, NULL, 0, NULL)) { failed = 1; break; }
                m->s.phase_index++;
                if (m->s.phase_index < m->s.phase_count) { m->s.phase_elapsed_h = 0; m->s.pause_elapsed_h = 0; }
                else if (!strcmp(m->s.type, "filter")) {
                    BFState affected = m->s; empty(&m->s);
                    if (emit(m, "COMPLETED", &affected, NULL, NULL, mono, NULL, 0, NULL)) failed = 1;
                } else {
                    m->s.phase_index = m->s.phase_count - 1;
                    copy(m->s.state, sizeof m->s.state, "READY_TO_TRANSFER");
                    m->s.hold_deadline_h = m->s.sim_time_h - remaining + m->s.hold_h;
                    if (emit(m, "READY_TO_TRANSFER", &m->s, NULL, NULL, mono, NULL, 0, NULL)) failed = 1;
                    if (!failed && m->s.sim_time_h > m->s.hold_deadline_h + 1e-9) failed = spoil(m, "hold_timeout", mono);
                }
            } else break;
        }
    }
    if (!failed && !m->s.spoiled && !m->s.flush_requested && enabled_h > 0 &&
        (is_state(m, "TRANSFERRING_IN") || is_state(m, "TRANSFERRING_OUT")) && !m->s.transfer_ready) {
        m->s.transfer_elapsed_h = fmin(m->s.transfer_duration_h, m->s.transfer_elapsed_h + enabled_h);
        if (m->s.transfer_elapsed_h + 1e-9 >= m->s.transfer_duration_h) {
            m->s.transfer_ready = 1;
            if (emit(m, "TRANSFER_READY", &m->s, NULL, NULL, mono, NULL, 0, NULL)) failed = 1;
        }
    }
    int flushing_before_tick = !strcmp(before.state, "FLUSH_PENDING") || !strcmp(before.state, "FLUSHING");
    if (!failed && enabled_h > 0 && flushing_before_tick && !active_transfer(m) && is_state(m, "FLUSH_PENDING")) {
        copy(m->s.state, sizeof m->s.state, "FLUSHING"); m->s.flush_elapsed_h = 0;
    }
    if (!failed && enabled_h > 0 && flushing_before_tick && is_state(m, "FLUSHING")) {
        m->s.flush_elapsed_h += enabled_h;
        if (m->s.flush_elapsed_h + 1e-9 >= m->s.flush_h) {
            BFState affected = m->s; empty(&m->s);
            if (emit(m, "FLUSHED", &affected, NULL, affected.reason, mono, NULL, 0, NULL)) failed = 1;
        }
    }
    int changed = m->s.machine_sequence != before.machine_sequence || strcmp(m->s.state, before.state);
    if (!failed && (changed || mono - m->last_checkpoint >= 0.25)) {
        failed = save(m, mono); if (!failed) m->last_checkpoint = mono;
    }
    if (failed || sql(m, "COMMIT")) { sql(m, "ROLLBACK"); m->s = before; m->last_mono = old_mono; return -1; }
    return 0;
}
int bf_ack(BFMachine *m, const char *event_id) {
    sqlite3_stmt *q; if (prepare(m, &q, "DELETE FROM outbox WHERE event_id=?")) return -1;
    sqlite3_bind_text(q, 1, event_id, -1, SQLITE_STATIC); return done(m, q);
}
int bf_outbox(BFMachine *m, int limit, int (*send)(const char *, void *), void *ctx) {
    sqlite3_stmt *q; if (prepare(m, &q, "SELECT payload FROM outbox ORDER BY seq LIMIT ?")) return -1;
    sqlite3_bind_int(q, 1, limit); int n = 0;
    while (sqlite3_step(q) == SQLITE_ROW) {
        if (send((const char *)sqlite3_column_text(q, 0), ctx)) break;
        n++;
    }
    sqlite3_finalize(q); return n;
}
