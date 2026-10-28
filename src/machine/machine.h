#ifndef BEER_MACHINE_H
#define BEER_MACHINE_H

#include <stddef.h>
#include <stdint.h>

#define BF_ID 128
#define BF_NAME 48
#define BF_JSON 16384
#define BF_SCHEMA_VERSION 2

/** Technical equipment state only. The scheduler owns all production/material associations. */
typedef struct {
    char machine_id[BF_ID];
    char type[BF_NAME];
    char boot_id[BF_ID];
    char state[BF_NAME];
    char current_command_id[BF_ID];
    char interrupted_command_id[BF_ID];
    char flush_command_id[BF_ID];
    char transfer_role[BF_NAME];
    char last_permit_id[BF_ID];
    double duration_h;
    double elapsed_h;
    double target_temp_c;
    double flush_duration_h;
    double sim_time_h;
    double time_scale;
    double last_wall;
    double last_permit_sim_time_h;
    int occupied;
    int local_stop;
    int global_stop;
    int transfer_ready;
    int flush_requested;
    uint64_t epoch;
    uint64_t state_version;
    uint64_t machine_sequence;
} BFState;

/** One actuator request; IDs identify commands/controllers, never production entities. */
typedef struct {
    char command_id[BF_ID];
    char cmd[BF_NAME];
    char machine_id[BF_ID];
    char controller_boot_id[BF_ID];
    char role[BF_NAME];
    char scope[BF_NAME];
    int local;
    int has_expected_version;
    int has_duration;
    int has_temperature;
    uint64_t epoch;
    uint64_t expected_version;
    double duration_h;
    double target_temp_c;
    double permit_ms;
    double sim_time_h;
    double time_scale;
} BFCommand;

typedef struct BFPendingEvent BFPendingEvent;
typedef struct BFCommandResult BFCommandResult;

/** Single-thread-owned RAM. Closing/restarting the process discards every member. */
typedef struct {
    BFState s;
    BFPendingEvent *pending_events;
    BFCommandResult *command_results;
    double last_mono;
    double permit_until;
    double default_flush_h;
    char error[256];
} BFMachine;

/** Initialize EMPTY with a fresh boot ID and no execution permit. */
int bf_init(BFMachine *machine, const char *id, const char *type, double time_scale, double flush_h,
            double mono, double wall);
/** Free pending events and deduplication results; no state is written anywhere. */
void bf_close(BFMachine *machine);
/** Apply one command: 0 accepted/replayed, 1 rejected, -1 internal failure. */
int bf_command(BFMachine *machine, const BFCommand *command, double mono, double wall,
               char *response, size_t capacity);
/** Advance the equipment timer only over the permitted part of the elapsed interval. */
int bf_tick(BFMachine *machine, double mono, double wall);
/** Serialize a read-only equipment observation. */
int bf_snapshot(const BFMachine *machine, double mono, char *out, size_t capacity);
/** Acknowledge one RAM-queued event; duplicate acknowledgements are harmless. */
int bf_ack(BFMachine *machine, const char *event_id);
/** Retry pending events in creation order without removing them. */
int bf_outbox(BFMachine *machine, int limit, int (*send)(const char *, void *), void *context);
/** Publish events after a connection-local sequence cursor, preserving order. */
int bf_outbox_new(BFMachine *machine, uint64_t *after_sequence, int limit,
                  int (*send)(const char *, void *), void *context);
/** Check both STOP latches and the volatile lease. */
int bf_permitted(const BFMachine *machine, double mono);
/** Validate one of the six configured equipment types. */
int bf_valid_type(const char *type);
/** Generate a random boot/event/physical-command UUID. */
void bf_uuid(char *out, size_t capacity);
/** Format an event's wall-clock timestamp. */
void bf_utc(double wall, char *out, size_t capacity);
/** Escape a JSON string into a bounded caller-owned buffer. */
int bf_json_string(const char *value, char *out, size_t capacity);

#endif
