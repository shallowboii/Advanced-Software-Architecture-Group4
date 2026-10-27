#ifndef BEER_MACHINE_H
#define BEER_MACHINE_H

#include <stddef.h>
#include <stdint.h>
#include <sqlite3.h>

#define BF_ID 128
#define BF_NAME 48
#define BF_PHASES 8
#define BF_JSON 32768

typedef struct {
    char name[BF_NAME];
    double duration_h;
    double target_temp_c;
} BFPhase;

typedef struct {
    char machine_id[BF_ID], type[BF_NAME], boot_id[BF_ID];
    char state[BF_NAME], batch_id[BF_ID], execution_id[BF_ID];
    char recipe_version_id[BF_ID], run_id[BF_ID];
    char transfer_id[BF_ID], transfer_role[BF_NAME], peer_id[BF_ID];
    char last_permit_id[BF_ID], reason[BF_ID];
    BFPhase phases[BF_PHASES];
    int phase_count, phase_index;
    double phase_elapsed_h, transfer_elapsed_h, transfer_duration_h;
    double hold_h, hold_deadline_h, max_pause_h, pause_elapsed_h;
    double flush_h, flush_elapsed_h, sim_time_h, time_scale, last_wall, last_permit_sim_time_h;
    int local_stop, global_stop, transfer_ready, flush_requested, spoiled;
    uint64_t epoch, state_version, machine_sequence;
} BFState;

typedef struct {
    char command_id[BF_ID], cmd[BF_NAME], machine_id[BF_ID];
    char batch_id[BF_ID], execution_id[BF_ID], recipe_version_id[BF_ID], run_id[BF_ID];
    char transfer_id[BF_ID], role[BF_NAME], peer_id[BF_ID], scope[BF_NAME], reason[BF_ID], discard_reason[BF_ID];
    BFPhase phases[BF_PHASES];
    int phase_count, local, has_expected_version, has_flush_h;
    uint64_t epoch, expected_version;
    double duration_h, hold_h, max_pause_h, permit_ms, sim_time_h, time_scale, flush_h;
} BFCommand;

typedef struct {
    sqlite3 *db;
    BFState s;
    double last_mono, permit_until, last_checkpoint, default_flush_h;
    char error[256];
} BFMachine;

int bf_open(BFMachine *m, const char *db_path, const char *id, const char *type,
            double time_scale, double flush_h, double mono, double wall);
int bf_checkpoint(BFMachine *m, double mono);
void bf_close(BFMachine *m);
int bf_command(BFMachine *m, const BFCommand *c, double mono, double wall,
               char *response, size_t response_cap);
int bf_tick(BFMachine *m, double mono, double wall);
int bf_snapshot(const BFMachine *m, double mono, char *out, size_t cap);
int bf_ack(BFMachine *m, const char *event_id);
int bf_outbox(BFMachine *m, int limit, int (*send)(const char *, void *), void *ctx);
int bf_permitted(const BFMachine *m, double mono);
int bf_valid_type(const char *type);
void bf_uuid(char *out, size_t cap);
void bf_utc(double wall, char *out, size_t cap);
int bf_json_string(const char *value, char *out, size_t cap);

#endif
