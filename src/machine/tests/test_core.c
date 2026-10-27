#define _POSIX_C_SOURCE 200809L
#include "machine.h"
#include <assert.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static unsigned serial;
static double now = 100;
static char response[BF_JSON];
static BFCommand command(BFMachine *m, const char *name) {
    BFCommand c; memset(&c, 0, sizeof c);
    snprintf(c.command_id, sizeof c.command_id, "command-%u", ++serial);
    snprintf(c.cmd, sizeof c.cmd, "%s", name);
    snprintf(c.machine_id, sizeof c.machine_id, "%s", m->s.machine_id);
    c.epoch = m->s.epoch; return c;
}
static int send(BFMachine *m, BFCommand *c) {
    int rc = bf_command(m, c, now, 100000 + now, response, sizeof response);
    if (rc < 0) fprintf(stderr, "Controller error: %s\n", m->error);
    assert(rc >= 0); return rc;
}
static void init(BFMachine *m, const char *type, const char *id) {
    assert(!bf_open(m, ":memory:", id, type, 1, .2, now, 100000+now));
}
static void permit(BFMachine *m) {
    BFCommand c = command(m, "PERMIT"); c.epoch = m->s.epoch ? m->s.epoch : 1;
    c.permit_ms = 10000; c.time_scale = 1; c.sim_time_h = now;
    assert(!send(m, &c));
}
static void tick(BFMachine *m, double dt) {
    now += dt; assert(!bf_tick(m, now, 100000+now));
}
static BFCommand stage(BFMachine *m, const char *cmd) {
    BFCommand c = command(m, cmd); strcpy(c.batch_id, "batch-1"); strcpy(c.execution_id, "exec-1");
    strcpy(c.recipe_version_id, "lager-v1"); strcpy(c.run_id, "run-1");
    c.phase_count = 1; strcpy(c.phases[0].name, "process"); c.phases[0].duration_h = 1;
    c.phases[0].target_temp_c = 65; c.hold_h = 20; c.max_pause_h = 20; return c;
}
static void start(BFMachine *m) { permit(m); BFCommand c = stage(m, "START"); assert(!send(m, &c)); }
static int count_event(BFMachine *m, const char *event) {
    sqlite3_stmt *q; assert(sqlite3_prepare_v2(m->db, "SELECT count(*) FROM outbox WHERE json_extract(payload,'$.event')=?", -1, &q, NULL) == SQLITE_OK);
    sqlite3_bind_text(q, 1, event, -1, SQLITE_STATIC); assert(sqlite3_step(q) == SQLITE_ROW);
    int n = sqlite3_column_int(q, 0); sqlite3_finalize(q); return n;
}
static void scopes_and_expiry(void) {
    BFMachine m; init(&m, "mash", "mash-0"); start(&m); tick(&m, .2);
    BFCommand c = command(&m, "STOP"); c.local = 1; strcpy(c.scope, "local"); assert(!send(&m, &c));
    c = command(&m, "STOP"); strcpy(c.scope, "global"); assert(!send(&m, &c));
    tick(&m, .3); assert(fabs(m.s.phase_elapsed_h - .2) < 1e-6);
    c = command(&m, "RESUME"); strcpy(c.scope, "global"); assert(!send(&m, &c));
    tick(&m, .3); assert(m.s.local_stop && !m.s.global_stop); assert(fabs(m.s.phase_elapsed_h - .2) < 1e-6);
    c = command(&m, "RESUME"); c.local = 1; strcpy(c.scope, "local"); assert(!send(&m, &c));
    tick(&m, .8); assert(!strcmp(m.s.state, "READY_TO_TRANSFER"));
    assert(count_event(&m, "PHASE_COMPLETED") == 1); assert(count_event(&m, "READY_TO_TRANSFER") == 1);
    bf_close(&m);
    init(&m, "mash", "mash-0"); permit(&m); c = stage(&m, "START"); c.phases[0].duration_h = 100; assert(!send(&m, &c));
    tick(&m, 12); assert(fabs(m.s.phase_elapsed_h - 10) < 1e-6); assert(!bf_permitted(&m, now));
    assert(fabs(m.s.pause_elapsed_h - 2) < 1e-6); bf_close(&m);
}
static void duplicate_and_fencing(void) {
    BFMachine m; init(&m, "mash", "mash-0"); permit(&m);
    BFCommand c = stage(&m, "START"); assert(!send(&m, &c));
    char original[BF_JSON]; strcpy(original, response); uint64_t seq = m.s.machine_sequence;
    assert(!send(&m, &c)); assert(!strcmp(original, response)); assert(m.s.machine_sequence == seq);
    strcpy(c.batch_id, "other"); assert(send(&m, &c) == 1); assert(strstr(response, "command_id_conflict"));
    BFCommand old = command(&m, "PERMIT"); old.epoch = 1; old.permit_ms = 3000; old.time_scale = 1; old.sim_time_h = now + .1;
    assert(!send(&m, &old)); double until = m.permit_until; now += .2;
    assert(!send(&m, &old)); assert(m.permit_until == until);
    BFCommand fresh = command(&m, "PERMIT"); fresh.epoch = 2; fresh.permit_ms = 3000; fresh.time_scale = 1; fresh.sim_time_h = now;
    assert(!send(&m, &fresh)); assert(send(&m, &old) == 1);
    c = command(&m, "RESUME"); c.epoch = 1; strcpy(c.scope, "global"); assert(send(&m, &c) == 1);
    c = command(&m, "STOP"); c.epoch = 1; strcpy(c.scope, "global"); assert(!send(&m, &c)); assert(m.s.global_stop);
    bf_close(&m);
}
static void spoil_while_stopped_and_flush(void) {
    BFMachine m; init(&m, "mash", "mash-0"); permit(&m);
    BFCommand c = stage(&m, "START"); c.hold_h = .5; assert(!send(&m, &c)); tick(&m, 1);
    c = command(&m, "STOP"); c.local = 1; strcpy(c.scope, "local"); assert(!send(&m, &c));
    tick(&m, 1); assert(m.s.spoiled); assert(!strcmp(m.s.state, "FLUSH_PENDING"));
    tick(&m, 1); assert(count_event(&m, "SPOILED") == 1); assert(*m.s.batch_id);
    c = command(&m, "RESUME"); c.local = 1; strcpy(c.scope, "local"); assert(!send(&m, &c));
    permit(&m); tick(&m, .3); assert(!strcmp(m.s.state, "EMPTY")); assert(count_event(&m, "FLUSHED") == 1);
    tick(&m, .5); assert(count_event(&m, "FLUSHED") == 1); bf_close(&m);
}
static void unitank_and_filter(void) {
    BFMachine m; init(&m, "unitank", "unitank-0"); permit(&m);
    BFCommand c = stage(&m, "START"); assert(send(&m, &c) == 1);
    c = stage(&m, "START"); c.phase_count = 2; strcpy(c.phases[0].name, "fermentation");
    c.phases[1] = c.phases[0]; strcpy(c.phases[1].name, "maturation"); assert(!send(&m, &c));
    tick(&m, 1.2); assert(m.s.phase_index == 1); assert(!strcmp(m.s.state, "PROCESSING"));
    assert(!strcmp(m.s.batch_id, "batch-1")); assert(count_event(&m, "READY_TO_TRANSFER") == 0);
    tick(&m, .8); assert(!strcmp(m.s.state, "READY_TO_TRANSFER")); assert(count_event(&m, "PHASE_COMPLETED") == 2);
    bf_close(&m); init(&m, "filter", "filter-0"); start(&m); tick(&m, 1);
    assert(!strcmp(m.s.state, "EMPTY")); assert(count_event(&m, "COMPLETED") == 1); bf_close(&m);
}
static void prepare_pair(BFMachine *a, BFMachine *b) {
    init(a, "mash", "mash-0"); init(b, "lauter", "lauter-0"); start(a); tick(a, 1); permit(b);
    BFCommand c = stage(a, "PREPARE_TRANSFER"); strcpy(c.transfer_id, "transfer-1"); strcpy(c.role, "source"); strcpy(c.peer_id, b->s.machine_id); assert(!send(a, &c));
    c = stage(b, "PREPARE_TRANSFER"); strcpy(c.transfer_id, "transfer-1"); strcpy(c.role, "target"); strcpy(c.peer_id, a->s.machine_id); assert(!send(b, &c));
}
static BFCommand transfer_cmd(BFMachine *m, const char *cmd) {
    BFCommand c = command(m, cmd); strcpy(c.transfer_id, "transfer-1"); c.duration_h = .5; return c;
}
static void transfer_and_discard_race(void) {
    BFMachine a, b; prepare_pair(&a, &b);
    BFCommand c = transfer_cmd(&b, "COMMIT_TRANSFER"); assert(send(&b, &c) == 1);
    c = transfer_cmd(&a, "EXECUTE_TRANSFER"); assert(!send(&a, &c));
    c = transfer_cmd(&b, "EXECUTE_TRANSFER"); assert(!send(&b, &c));
    now += .5; assert(!bf_tick(&a, now, 100000+now)); assert(!bf_tick(&b, now, 100000+now));
    assert(a.s.transfer_ready && b.s.transfer_ready); assert(!strcmp(b.s.state, "TRANSFERRING_IN"));
    c = command(&a, "STOP"); c.local = 1; strcpy(c.scope, "local"); assert(!send(&a, &c));
    c = command(&a, "FLUSH"); c.local = 1; strcpy(c.batch_id, "batch-1"); strcpy(c.reason, "operator"); assert(!send(&a, &c));
    tick(&a, .1); assert(*a.s.batch_id && a.s.flush_requested); assert(count_event(&a, "FLUSHED") == 0);
    c = transfer_cmd(&a, "COMMIT_TRANSFER"); assert(!send(&a, &c));
    assert(!strcmp(a.s.state, "EMPTY")); assert(strstr(response, "\"discard_reason\":\"operator\""));
    assert(strstr(response, "\"batch_id\":\"batch-1\""));
    c = transfer_cmd(&b, "COMMIT_TRANSFER"); strcpy(c.discard_reason, "operator"); assert(!send(&b, &c));
    assert(!strcmp(b.s.state, "FLUSH_PENDING")); assert(strstr(response, "\"discard_reason\":\"operator\""));
    tick(&b, .3); assert(!strcmp(b.s.state, "EMPTY")); assert(count_event(&b, "FLUSHED") == 1);
    bf_close(&a); bf_close(&b);
}
static void abort_flush_inflight(void) {
    BFMachine a, b; prepare_pair(&a, &b);
    BFCommand c = transfer_cmd(&a, "EXECUTE_TRANSFER"); assert(!send(&a, &c));
    c = transfer_cmd(&b, "EXECUTE_TRANSFER"); assert(!send(&b, &c));
    tick(&a, .1);
    c = command(&a, "FLUSH"); c.local = 1; strcpy(c.batch_id, "batch-1"); assert(!send(&a, &c));
    tick(&a, 1); assert(!a.s.transfer_ready); assert(*a.s.batch_id);
    c = transfer_cmd(&a, "ABORT_TRANSFER"); assert(!send(&a, &c)); assert(!strcmp(a.s.state, "FLUSH_PENDING"));
    c = transfer_cmd(&b, "ABORT_TRANSFER"); assert(!send(&b, &c)); assert(!strcmp(b.s.state, "EMPTY"));
    tick(&a, .3); assert(!strcmp(a.s.state, "EMPTY")); bf_close(&a); bf_close(&b);
}
static void restart_and_ack(void) {
    char path[] = "/tmp/beer-machine-test-XXXXXX"; int fd = mkstemp(path); assert(fd >= 0); close(fd);
    BFMachine m; assert(!bf_open(&m, path, "mash-0", "mash", 1, .2, now, 100000+now)); permit(&m);
    BFCommand c = stage(&m, "START"); c.phases[0].duration_h = 100; c.max_pause_h = .5; assert(!send(&m, &c));
    char original[BF_JSON]; strcpy(original, response);
    sqlite3_stmt *q; assert(sqlite3_prepare_v2(m.db, "SELECT event_id FROM outbox LIMIT 1", -1, &q, NULL) == SQLITE_OK);
    assert(sqlite3_step(q) == SQLITE_ROW); char event[128]; snprintf(event, sizeof event, "%s", sqlite3_column_text(q,0)); sqlite3_finalize(q);
    assert(!bf_ack(&m, event)); assert(count_event(&m, "STARTED") == 0);
    assert(!send(&m, &c)); assert(!strcmp(original, response)); assert(count_event(&m, "STARTED") == 1);
    tick(&m, .3); double progress = m.s.phase_elapsed_h; char boot[BF_ID]; strcpy(boot, m.s.boot_id); bf_close(&m);
    now += 1; assert(!bf_open(&m, path, "mash-0", "mash", 1, .2, now, 100000+now));
    assert(strcmp(boot, m.s.boot_id)); assert(!bf_permitted(&m, now)); assert(fabs(progress - m.s.phase_elapsed_h) < 1e-6);
    assert(!bf_tick(&m, now, 100000+now)); assert(m.s.spoiled); assert(!strcmp(m.s.reason, "interruption_limit"));
    assert(!send(&m, &c)); assert(!strcmp(original, response)); assert(m.s.spoiled); bf_close(&m); unlink(path);
}
static void aborted_reservation_tombstone(void) {
    BFMachine m; init(&m, "lauter", "lauter-0"); permit(&m);
    BFCommand abort = transfer_cmd(&m, "ABORT_TRANSFER"); strcpy(abort.batch_id, "batch-1");
    assert(!send(&m, &abort)); assert(strstr(response, "\"transfer_id\":\"transfer-1\""));
    BFCommand prepare = stage(&m, "PREPARE_TRANSFER"); strcpy(prepare.transfer_id, "transfer-1");
    strcpy(prepare.role, "target"); strcpy(prepare.peer_id, "mash-0");
    assert(send(&m, &prepare) == 1); assert(strstr(response, "transfer_already_decided"));
    assert(!strcmp(m.s.state, "EMPTY"));
    abort.epoch = 999; assert(!send(&m, &abort)); /* cached result survives epoch handover */
    assert(count_event(&m, "TRANSFER_ABORTED") == 1); bf_close(&m);
}
static void abort_unprepared_source(void) {
    BFMachine m; init(&m, "mash", "mash-0"); start(&m); tick(&m, 1);
    BFCommand stop = command(&m, "STOP"); strcpy(stop.scope, "local"); assert(!send(&m, &stop));
    BFCommand prepare = stage(&m, "PREPARE_TRANSFER"); strcpy(prepare.transfer_id, "transfer-1");
    strcpy(prepare.role, "source"); strcpy(prepare.peer_id, "lauter-0"); assert(send(&m, &prepare) == 1);
    BFCommand abort = transfer_cmd(&m, "ABORT_TRANSFER"); assert(!send(&m, &abort));
    assert(!strcmp(m.s.state, "READY_TO_TRANSFER")); assert(!strcmp(m.s.batch_id, "batch-1"));
    assert(strstr(response, "\"transfer_id\":\"transfer-1\""));
    assert(strstr(response, "\"batch_id\":null")); /* coordinator resolves through transfer ID */
    bf_close(&m);
}
static void transfer_stop_and_healthy_commit(void) {
    BFMachine a, b; prepare_pair(&a, &b);
    BFCommand ca = transfer_cmd(&a, "EXECUTE_TRANSFER"), cb = transfer_cmd(&b, "EXECUTE_TRANSFER");
    assert(!send(&a, &ca)); assert(!send(&b, &cb));
    now += .2; assert(!bf_tick(&a, now, 100000+now)); assert(!bf_tick(&b, now, 100000+now));
    ca = command(&a, "STOP"); strcpy(ca.scope, "global"); assert(!send(&a, &ca));
    cb = command(&b, "STOP"); strcpy(cb.scope, "global"); assert(!send(&b, &cb));
    now += .4; assert(!bf_tick(&a, now, 100000+now)); assert(!bf_tick(&b, now, 100000+now));
    assert(fabs(a.s.transfer_elapsed_h - .2) < 1e-6); assert(fabs(b.s.transfer_elapsed_h - .2) < 1e-6);
    ca = command(&a, "RESUME"); strcpy(ca.scope, "global"); assert(!send(&a, &ca));
    cb = command(&b, "RESUME"); strcpy(cb.scope, "global"); assert(!send(&b, &cb));
    now += .3; assert(!bf_tick(&a, now, 100000+now)); assert(!bf_tick(&b, now, 100000+now));
    assert(a.s.transfer_ready && b.s.transfer_ready);
    ca = transfer_cmd(&a, "COMMIT_TRANSFER"); assert(!send(&a, &ca));
    cb = transfer_cmd(&b, "COMMIT_TRANSFER"); assert(!send(&b, &cb));
    assert(!*a.s.batch_id && !strcmp(b.s.batch_id, "batch-1"));
    assert(!strcmp(b.s.state, "PROCESSING")); assert(b.s.phase_elapsed_h == 0);
    assert(!send(&a, &ca)); assert(!send(&b, &cb));
    assert(count_event(&a, "TRANSFER_COMMITTED") == 1); assert(count_event(&b, "TRANSFER_COMMITTED") == 1);
    bf_close(&a); bf_close(&b);
}
static void repeated_flush_preserves_progress(void) {
    BFMachine m; init(&m, "mash", "mash-0"); start(&m); m.s.flush_h = 2;
    BFCommand c = command(&m, "FLUSH"); strcpy(c.batch_id, "batch-1"); strcpy(c.reason, "operator");
    assert(!send(&m, &c)); tick(&m, .8); assert(!strcmp(m.s.state, "FLUSHING"));
    c = command(&m, "FLUSH"); strcpy(c.batch_id, "batch-1"); strcpy(c.reason, "different");
    assert(!send(&m, &c)); assert(strstr(response, "\"event\":\"SNAPSHOT\""));
    assert(fabs(m.s.flush_elapsed_h - .8) < 1e-6); assert(!strcmp(m.s.reason, "operator"));
    tick(&m, 1.2); assert(!strcmp(m.s.state, "EMPTY"));
    assert(count_event(&m, "FLUSH_REQUESTED") == 1); assert(count_event(&m, "FLUSHED") == 1);
    bf_close(&m);
}
static void recipe_flush_duration(void) {
    char path[] = "/tmp/beer-machine-recipe-XXXXXX"; int fd = mkstemp(path); assert(fd >= 0); close(fd);
    BFMachine m; assert(!bf_open(&m, path, "mash-0", "mash", 1, .2, now, 100000+now)); permit(&m);
    BFCommand c = stage(&m, "START"); c.has_flush_h = 1; c.flush_h = 2; assert(!send(&m, &c));
    assert(m.s.flush_h == 2); c.flush_h = 3; assert(send(&m, &c) == 1); assert(strstr(response, "command_id_conflict"));
    bf_close(&m); assert(!bf_open(&m, path, "mash-0", "mash", 1, .4, now, 100000+now));
    assert(m.s.flush_h == 2); now += .1; permit(&m);
    c = command(&m, "FLUSH"); strcpy(c.batch_id, "batch-1"); assert(!send(&m, &c));
    tick(&m, 1); assert(!strcmp(m.s.state, "FLUSHING")); tick(&m, 1); assert(!strcmp(m.s.state, "EMPTY"));
    c = stage(&m, "START"); assert(!send(&m, &c)); assert(m.s.flush_h == .4);
    bf_close(&m); unlink(path);
    init(&m, "lauter", "lauter-0"); permit(&m);
    c = stage(&m, "PREPARE_TRANSFER"); strcpy(c.transfer_id, "new-transfer"); strcpy(c.role, "target"); strcpy(c.peer_id, "mash-0");
    c.has_flush_h = 1; c.flush_h = .75; assert(!send(&m, &c)); assert(m.s.flush_h == .75); bf_close(&m);
}
int main(void) {
    scopes_and_expiry(); duplicate_and_fencing(); spoil_while_stopped_and_flush();
    unitank_and_filter(); transfer_and_discard_race(); abort_flush_inflight(); restart_and_ack(); aborted_reservation_tombstone(); transfer_stop_and_healthy_commit(); abort_unprepared_source(); repeated_flush_preserves_progress(); recipe_flush_duration();
    puts("PASS: 12 core scenarios (durability, replay, fencing, STOP, expiry, phases, transfers, discard races)");
    return 0;
}
