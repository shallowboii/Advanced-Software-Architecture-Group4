#include "machine.h"
#include <assert.h>
#include <math.h>
#include <stdio.h>
#include <string.h>

static unsigned serial;
static double now = 100;
static char response[BF_JSON];

typedef struct {
    const char *kind;
    int count;
    int fail;
    char first_id[BF_ID];
    char last[BF_JSON];
} Capture;

/** Inspect public event output, optionally simulating a refused network enqueue. */
static int capture(const char *payload, void *context) {
    Capture *result = context;
    if (result->fail)
        return -1;
    char pattern[128];
    snprintf(pattern, sizeof pattern, "\"event\":\"%s\"", result->kind ? result->kind : "");
    if (!result->kind || strstr(payload, pattern)) {
        result->count++;
        snprintf(result->last, sizeof result->last, "%s", payload);
        if (!*result->first_id) {
            const char *id = strstr(payload, "\"event_id\":\"");
            assert(id);
            id += strlen("\"event_id\":\"");
            const char *end = strchr(id, '"');
            assert(end && (size_t)(end - id) < sizeof result->first_id);
            memcpy(result->first_id, id, (size_t)(end - id));
        }
    }
    return 0;
}

/** Count pending transitions without inspecting the RAM-list implementation. */
static int event_count(BFMachine *machine, const char *kind) {
    Capture result = {.kind = kind};
    bf_outbox(machine, 1000, capture, &result);
    return result.count;
}

/** Construct a uniquely identified command fenced to the current equipment observation. */
static BFCommand command(BFMachine *machine, const char *name) {
    BFCommand result = {0};
    snprintf(result.command_id, sizeof result.command_id, "command-%u", ++serial);
    snprintf(result.machine_id, sizeof result.machine_id, "%s", machine->s.machine_id);
    snprintf(result.controller_boot_id, sizeof result.controller_boot_id, "%s", machine->s.boot_id);
    snprintf(result.cmd, sizeof result.cmd, "%s", name);
    result.epoch = machine->s.epoch;
    result.has_expected_version = 1;
    result.expected_version = machine->s.state_version;
    return result;
}

/** Apply a deterministic request and fail the test on internal errors, not normal rejection. */
static int send(BFMachine *machine, BFCommand *request) {
    int result = bf_command(machine, request, now, 100000 + now, response, sizeof response);
    assert(result >= 0);
    return result;
}

/** Create empty equipment with a short configured flush duration. */
static void init(BFMachine *machine, const char *type, const char *id) {
    assert(!bf_init(machine, id, type, 1, .2, now, 100000 + now));
}

/** Renew a finite lease without resetting latches or structural versions. */
static void permit(BFMachine *machine) {
    BFCommand request = command(machine, "PERMIT");
    request.epoch = machine->s.epoch ? machine->s.epoch : 1;
    request.permit_ms = 10000;
    request.time_scale = 1;
    request.sim_time_h = now;
    assert(!send(machine, &request));
}

/** Run one simulated clock interval while preserving deterministic test timing. */
static void tick(BFMachine *machine, double duration) {
    now += duration;
    assert(!bf_tick(machine, now, 100000 + now));
}

/** Build one scalar operation; the controller receives no recipe or named phase. */
static BFCommand operation(BFMachine *machine, const char *name, double duration) {
    BFCommand request = command(machine, name);
    request.has_duration = request.has_temperature = 1;
    request.duration_h = duration;
    request.target_temp_c = 65;
    return request;
}

/** Load empty equipment and start one ordinary operation. */
static void start(BFMachine *machine, double duration) {
    permit(machine);
    BFCommand request = operation(machine, "START", duration);
    assert(!send(machine, &request));
}

/** Verify STOP latches are independent and actuator correlation survives control/permit updates. */
static void independent_stop_latches(void) {
    BFMachine machine;
    init(&machine, "mash", "mash-0");
    start(&machine, 5);
    char active[BF_ID];
    strcpy(active, machine.s.current_command_id);
    tick(&machine, .2);
    BFCommand request = command(&machine, "STOP");
    strcpy(request.scope, "local");
    assert(!send(&machine, &request));
    request = command(&machine, "STOP");
    strcpy(request.scope, "global");
    assert(!send(&machine, &request));
    tick(&machine, 20);
    assert(fabs(machine.s.elapsed_h - .2) < 1e-6);
    request = command(&machine, "RESUME");
    strcpy(request.scope, "global");
    assert(!send(&machine, &request));
    permit(&machine);
    tick(&machine, .1);
    assert(machine.s.local_stop && !machine.s.global_stop);
    assert(fabs(machine.s.elapsed_h - .2) < 1e-6);
    request = command(&machine, "RESUME");
    strcpy(request.scope, "local");
    assert(!send(&machine, &request));
    tick(&machine, .3);
    assert(fabs(machine.s.elapsed_h - .5) < 1e-6);
    assert(!strcmp(active, machine.s.current_command_id));
    assert(machine.s.occupied);
    bf_close(&machine);
}

/** Verify elapsed work stops exactly at lease expiry, even when a tick spans the boundary. */
static void expiring_permit(void) {
    BFMachine machine;
    init(&machine, "mash", "mash-0");
    start(&machine, 100);
    uint64_t version = machine.s.state_version;
    tick(&machine, 12);
    assert(fabs(machine.s.elapsed_h - 10) < 1e-6);
    assert(!bf_permitted(&machine, now));
    assert(machine.s.state_version == version);
    assert(!strcmp(machine.s.state, "PROCESSING"));
    bf_close(&machine);
}

/** Verify timer completion retains contents until a separate scalar operation or release command.
 */
static void scheduler_sequences_operations(void) {
    BFMachine machine;
    init(&machine, "unitank", "unitank-0");
    start(&machine, 1);
    char first[BF_ID];
    strcpy(first, machine.s.current_command_id);
    tick(&machine, 1);
    assert(!strcmp(machine.s.state, "READY") && machine.s.occupied);
    tick(&machine, 2);
    assert(!strcmp(machine.s.current_command_id, first));
    assert(event_count(&machine, "PROCESS_COMPLETED") == 1);
    BFCommand request = operation(&machine, "START", 1);
    assert(send(&machine, &request) == 1);
    request = operation(&machine, "PROCESS", 2);
    request.target_temp_c = 2;
    assert(!send(&machine, &request));
    tick(&machine, 2);
    assert(machine.s.occupied && !strcmp(machine.s.state, "READY"));
    assert(event_count(&machine, "PROCESS_COMPLETED") == 2);
    bf_close(&machine);
    init(&machine, "filter", "filter-0");
    start(&machine, 1);
    tick(&machine, 1);
    assert(machine.s.occupied);
    request = command(&machine, "RELEASE");
    assert(!send(&machine, &request));
    assert(!machine.s.occupied && !strcmp(machine.s.state, "EMPTY"));
    assert(event_count(&machine, "RELEASED") == 1);
    bf_close(&machine);
}

/** Verify the operation result is replayed before version guards and conflicting content is
 * rejected. */
static void command_replay_and_ack(void) {
    BFMachine machine;
    init(&machine, "mash", "mash-0");
    permit(&machine);
    BFCommand request = operation(&machine, "START", 10);
    assert(!send(&machine, &request));
    char original[BF_JSON];
    strcpy(original, response);
    Capture result = {.kind = "PROCESS_STARTED"};
    bf_outbox(&machine, 100, capture, &result);
    assert(result.count == 1);
    bf_ack(&machine, result.first_id);
    assert(event_count(&machine, "PROCESS_STARTED") == 0);
    request.epoch++;
    assert(!send(&machine, &request));
    assert(!strcmp(response, original));
    assert(event_count(&machine, "PROCESS_STARTED") == 1);
    request.duration_h = 11;
    assert(send(&machine, &request) == 1);
    assert(strstr(response, "command_id_conflict"));
    bf_close(&machine);
}

/** Verify old permits neither extend the lease nor override a newer coordinator epoch. */
static void permit_replay_and_epoch(void) {
    BFMachine machine;
    init(&machine, "mash", "mash-0");
    BFCommand original = command(&machine, "PERMIT");
    original.epoch = 1;
    original.permit_ms = 3000;
    original.time_scale = 1;
    original.sim_time_h = now;
    assert(!send(&machine, &original));
    double deadline = machine.permit_until;
    tick(&machine, .2);
    assert(!send(&machine, &original));
    assert(machine.permit_until == deadline);
    BFCommand newer = command(&machine, "PERMIT");
    newer.epoch = 2;
    newer.permit_ms = 3000;
    newer.time_scale = 1;
    newer.sim_time_h = now;
    assert(!send(&machine, &newer));
    assert(send(&machine, &original) == 1);
    BFCommand stale = operation(&machine, "START", 1);
    stale.epoch = 1;
    assert(send(&machine, &stale) == 1);
    assert(strstr(response, "stale_coordinator_epoch"));
    stale = command(&machine, "STOP");
    stale.epoch = 1;
    strcpy(stale.scope, "global");
    assert(!send(&machine, &stale));
    assert(machine.s.global_stop);
    bf_close(&machine);
}

/** Create two reserved pipe endpoints using only role, setpoints and command guards. */
static void reserved_pair(BFMachine *source, BFMachine *target) {
    init(source, "mash", "mash-0");
    init(target, "lauter", "lauter-0");
    start(source, 1);
    tick(source, 1);
    permit(target);
    BFCommand request = command(source, "PREPARE_TRANSFER");
    strcpy(request.role, "source");
    assert(!send(source, &request));
    request = command(target, "PREPARE_TRANSFER");
    strcpy(request.role, "target");
    assert(!send(target, &request));
}

/** Begin one timed pipe operation at both endpoints. */
static void execute_pair(BFMachine *source, BFMachine *target) {
    BFCommand request = command(source, "EXECUTE_TRANSFER");
    request.has_duration = 1;
    request.duration_h = .5;
    assert(!send(source, &request));
    request = command(target, "EXECUTE_TRANSFER");
    request.has_duration = 1;
    request.duration_h = .5;
    assert(!send(target, &request));
}

/** Advance both endpoint clocks over the same physical interval. */
static void tick_pair(BFMachine *source, BFMachine *target, double duration) {
    now += duration;
    assert(!bf_tick(source, now, 100000 + now));
    assert(!bf_tick(target, now, 100000 + now));
}

/** Verify commit requires completed pipe motion and receiver processing requires another command.
 */
static void transfer_requires_explicit_processing(void) {
    BFMachine source, target;
    reserved_pair(&source, &target);
    BFCommand request = command(&target, "COMMIT_TRANSFER");
    assert(send(&target, &request) == 1);
    execute_pair(&source, &target);
    tick_pair(&source, &target, .5);
    assert(source.s.transfer_ready && target.s.transfer_ready);
    request = command(&source, "COMMIT_TRANSFER");
    assert(!send(&source, &request));
    request = command(&target, "COMMIT_TRANSFER");
    assert(!send(&target, &request));
    assert(!source.s.occupied && target.s.occupied);
    assert(!strcmp(target.s.state, "READY"));
    tick(&target, .3);
    assert(!strcmp(target.s.state, "READY"));
    request = operation(&target, "PROCESS", 1);
    assert(!send(&target, &request));
    assert(!strcmp(target.s.state, "PROCESSING"));
    bf_close(&source);
    bf_close(&target);
}

/** Verify both pipe timers pause under global STOP without losing their actuator references. */
static void transfer_pauses(void) {
    BFMachine source, target;
    reserved_pair(&source, &target);
    execute_pair(&source, &target);
    tick_pair(&source, &target, .2);
    BFCommand request = command(&source, "STOP");
    strcpy(request.scope, "global");
    assert(!send(&source, &request));
    request = command(&target, "STOP");
    strcpy(request.scope, "global");
    assert(!send(&target, &request));
    tick_pair(&source, &target, 1);
    assert(fabs(source.s.elapsed_h - .2) < 1e-6 && fabs(target.s.elapsed_h - .2) < 1e-6);
    bf_close(&source);
    bf_close(&target);
}

/** Verify a physical flush during reconciliation is reported when the source later releases. */
static void flush_races_commit(void) {
    BFMachine source, target;
    reserved_pair(&source, &target);
    execute_pair(&source, &target);
    tick_pair(&source, &target, .5);
    BFCommand request = command(&source, "FLUSH");
    request.local = 1;
    request.has_expected_version = 0;
    assert(!send(&source, &request));
    assert(source.s.flush_requested && *source.s.interrupted_command_id);
    assert(event_count(&source, "FLUSHED") == 0);
    request = command(&source, "COMMIT_TRANSFER");
    assert(!send(&source, &request));
    assert(strstr(response, "\"contents_discarded\":true"));
    request = command(&target, "COMMIT_TRANSFER");
    assert(!send(&target, &request));
    assert(!strcmp(target.s.state, "READY"));
    request = command(&target, "FLUSH");
    assert(!send(&target, &request));
    tick(&target, .3);
    assert(!target.s.occupied);
    bf_close(&source);
    bf_close(&target);
}

/** Verify receiver flush does not silently progress a pipe and abort preserves the source contents.
 */
static void receiver_flush_then_abort(void) {
    BFMachine source, target;
    reserved_pair(&source, &target);
    execute_pair(&source, &target);
    BFCommand request = command(&target, "FLUSH");
    request.local = 1;
    request.has_expected_version = 0;
    assert(!send(&target, &request));
    tick_pair(&source, &target, 1);
    assert(!target.s.transfer_ready);
    request = command(&source, "ABORT_TRANSFER");
    assert(!send(&source, &request));
    request = command(&target, "ABORT_TRANSFER");
    assert(!send(&target, &request));
    assert(source.s.occupied && !strcmp(source.s.state, "READY"));
    assert(!target.s.occupied && !strcmp(target.s.state, "EMPTY"));
    bf_close(&source);
    bf_close(&target);
}

/** Verify an early abort's version transition prevents a delayed reservation from reappearing. */
static void abort_before_prepare(void) {
    BFMachine machine;
    init(&machine, "lauter", "lauter-0");
    permit(&machine);
    BFCommand late = command(&machine, "PREPARE_TRANSFER");
    strcpy(late.role, "target");
    BFCommand abort = command(&machine, "ABORT_TRANSFER");
    assert(!send(&machine, &abort));
    assert(send(&machine, &late) == 1);
    assert(strstr(response, "state_version_conflict"));
    assert(!strcmp(machine.s.state, "EMPTY"));
    bf_close(&machine);
}

/** Verify delayed network controls cannot modify a later equipment operation; physical input stays
 * current. */
static void stale_controls_are_fenced(void) {
    BFMachine machine;
    init(&machine, "mash", "mash-0");
    permit(&machine);
    BFCommand old_flush = command(&machine, "FLUSH");
    BFCommand start_request = operation(&machine, "START", 2);
    assert(!send(&machine, &start_request));
    assert(send(&machine, &old_flush) == 1);
    assert(!machine.s.flush_requested && machine.s.occupied);
    BFCommand unguarded = command(&machine, "ABORT_TRANSFER");
    unguarded.has_expected_version = 0;
    assert(send(&machine, &unguarded) == 1);
    assert(strstr(response, "expected_state_version_required"));
    BFCommand physical = command(&machine, "FLUSH");
    physical.local = 1;
    physical.has_expected_version = 0;
    assert(!send(&machine, &physical));
    assert(machine.s.flush_requested);
    bf_close(&machine);
}

/** Verify repeated flushes preserve duration/progress and STOP inhibits removal. */
static void repeated_flush_and_stop(void) {
    BFMachine machine;
    init(&machine, "mash", "mash-0");
    start(&machine, 10);
    BFCommand request = command(&machine, "FLUSH");
    request.has_duration = 1;
    request.duration_h = 2;
    assert(!send(&machine, &request));
    tick(&machine, .8);
    request = command(&machine, "FLUSH");
    assert(!send(&machine, &request));
    assert(strstr(response, "\"event\":\"SNAPSHOT\""));
    assert(fabs(machine.s.elapsed_h - .8) < 1e-6);
    request = command(&machine, "STOP");
    strcpy(request.scope, "local");
    assert(!send(&machine, &request));
    tick(&machine, .5);
    assert(fabs(machine.s.elapsed_h - .8) < 1e-6);
    request = command(&machine, "RESUME");
    strcpy(request.scope, "local");
    assert(!send(&machine, &request));
    tick(&machine, 1.2);
    assert(!machine.s.occupied);
    assert(event_count(&machine, "FLUSH_REQUESTED") == 1 && event_count(&machine, "FLUSHED") == 1);
    bf_close(&machine);
}

/** Verify restart loses RAM contents/history and old-boot commands/permits cannot start equipment.
 */
static void restart_is_empty_and_fenced(void) {
    BFMachine machine;
    init(&machine, "mash", "mash-0");
    permit(&machine);
    BFCommand old = operation(&machine, "START", 20);
    assert(!send(&machine, &old));
    tick(&machine, 1);
    char boot[BF_ID];
    strcpy(boot, machine.s.boot_id);
    bf_close(&machine);
    init(&machine, "mash", "mash-0");
    assert(strcmp(boot, machine.s.boot_id));
    assert(!machine.s.occupied && !strcmp(machine.s.state, "EMPTY"));
    assert(!*machine.s.current_command_id && !*machine.s.interrupted_command_id);
    assert(!machine.s.machine_sequence && !machine.s.state_version && !machine.s.elapsed_h);
    assert(!bf_permitted(&machine, now));
    assert(event_count(&machine, "PROCESS_STARTED") == 0);
    assert(send(&machine, &old) == 1);
    assert(strstr(response, "controller_boot_mismatch"));
    BFCommand stale = command(&machine, "PERMIT");
    strcpy(stale.controller_boot_id, boot);
    stale.epoch = 1;
    stale.permit_ms = 3000;
    stale.time_scale = 1;
    stale.sim_time_h = now;
    assert(send(&machine, &stale) == 1);
    assert(!bf_permitted(&machine, now));
    bf_close(&machine);
}

/** Verify reconnect drains existing RAM history before newer events and failed enqueue preserves
 * cursor. */
static void ordered_reconnect_replay(void) {
    BFMachine machine;
    init(&machine, "mash", "mash-0");
    for (int i = 0; i < 70; i++) {
        BFCommand request = command(&machine, "SNAPSHOT");
        assert(!send(&machine, &request));
    }
    BFCommand stop = command(&machine, "STOP");
    strcpy(stop.scope, "global");
    assert(!send(&machine, &stop));
    uint64_t cursor = 0;
    Capture result = {.fail = 1};
    assert(!bf_outbox_new(&machine, &cursor, 64, capture, &result));
    assert(!cursor);
    result.fail = 0;
    assert(bf_outbox_new(&machine, &cursor, 64, capture, &result) == 64);
    assert(cursor == 64 && !strstr(result.last, "STOPPED"));
    assert(bf_outbox_new(&machine, &cursor, 64, capture, &result) == 7);
    assert(cursor == 71 && strstr(result.last, "STOPPED"));
    permit(&machine);
    assert(machine.s.machine_sequence == cursor);
    assert(!bf_outbox_new(&machine, &cursor, 64, capture, &result));
    bf_close(&machine);
}

/** Run equipment semantics without database, broker, filesystem state or third-party libraries. */
int main(void) {
    independent_stop_latches();
    expiring_permit();
    scheduler_sequences_operations();
    command_replay_and_ack();
    permit_replay_and_epoch();
    transfer_requires_explicit_processing();
    transfer_pauses();
    flush_races_commit();
    receiver_flush_then_abort();
    abort_before_prepare();
    stale_controls_are_fenced();
    repeated_flush_and_stop();
    restart_is_empty_and_fenced();
    ordered_reconnect_replay();
    puts("PASS: 14 equipment scenarios (RAM lifetime, scalar operations, STOP, transfer, flush, "
         "fencing, replay)");
    return 0;
}
