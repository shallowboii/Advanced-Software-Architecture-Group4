#include "wire.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>

/** Parse one request using fixed equipment identity; tests exercise the production cJSON adapter.
 */
static int parse(const char *json, int local, BFCommand *command) {
    char error[128];
    return bf_parse_command(json, "mash-0", 1, local, command, error, sizeof error);
}

/** Ensure protocol-v2 boundaries reject business metadata, legacy payloads and malformed scalars.
 */
int main(void) {
    BFCommand command;
    const char *valid = "{\"schema_version\":2,\"command_id\":\"start-1\",\"cmd\":\"START\","
                        "\"machine_id\":\"mash-0\",\"controller_boot_id\":\"boot-1\",\"epoch\":1,"
                        "\"expected_state_version\":0,\"duration_h\":2,\"target_temp_c\":65}";
    assert(!parse(valid, 0, &command));
    assert(command.has_duration && command.has_temperature && command.duration_h == 2);
    assert(command.has_expected_version && command.expected_version == 0);
    assert(!strcmp(command.controller_boot_id, "boot-1"));
    const char *forbidden[] = {
        "batch_id", "execution_id", "recipe_version_id", "run_id", "transfer_id",
        "phases",   "hold_h",       "max_pause_h",       "reason", "discard_reason"};
    for (size_t i = 0; i < sizeof forbidden / sizeof *forbidden; i++) {
        char payload[2048];
        snprintf(payload, sizeof payload, "%.*s,\"%s\":\"unrelated\"}", (int)strlen(valid) - 1,
                 valid, forbidden[i]);
        assert(parse(payload, 0, &command) != 0);
    }
    assert(parse("{\"schema_version\":1,\"cmd\":\"START\",\"command_id\":\"x\",\"epoch\":1}", 0,
                 &command));
    assert(parse("{\"schema_version\":2,\"cmd\":\"STOP\",\"cmd\":\"START\",\"command_id\":\"x\","
                 "\"epoch\":1}",
                 0, &command));
    assert(parse("{\"schema_version\":2,\"cmd\":\"START\",\"command_id\":\"x\",\"epoch\":true}", 0,
                 &command));
    assert(parse("{\"schema_version\":2,\"cmd\":\"START\",\"command_id\":\"x\",\"epoch\":1,"
                 "\"duration_h\":null}",
                 0, &command));
    assert(parse("{\"schema_version\":2,\"cmd\":\"START\",\"command_id\":\"x\",\"epoch\":1,"
                 "\"duration_h\":1e999}",
                 0, &command));
    assert(parse("{\"cmd\":\"START\",\"duration_h\":1,\"target_temp_c\":65}", 1, &command));
    assert(!parse("{\"cmd\":\"FLUSH\",\"duration_h\":0.3}", 1, &command));
    assert(command.local && *command.command_id && command.has_duration);
    char event_id[BF_ID];
    assert(!bf_parse_ack("{\"event_id\":\"event-1\"}", event_id, sizeof event_id));
    assert(!strcmp(event_id, "event-1"));
    assert(bf_parse_ack("{\"event_id\":false}", event_id, sizeof event_id));
    puts("PASS: production wire parser accepts equipment v2 and rejects business "
         "metadata/malformed input");
    return 0;
}
