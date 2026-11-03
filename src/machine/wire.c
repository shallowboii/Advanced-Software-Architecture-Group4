#include "wire.h"
#include <cjson/cJSON.h>
#include <math.h>
#include <stdio.h>
#include <string.h>

/** Accept only equipment-protocol fields; unrelated business metadata is never ignored. */
static int known_fields(const cJSON *root) {
    static const char *allowed[] = {
        "schema_version",     "message_id",    "command_id", "machine_id",
        "controller_boot_id", "cmd",           "epoch",      "expected_state_version",
        "duration_h",         "target_temp_c", "role",       "scope",
        "permit_ms",          "sim_time_h",    "time_scale", "ts_utc"};
    for (const cJSON *field = root->child; field; field = field->next) {
        int found = 0;
        for (size_t i = 0; i < sizeof allowed / sizeof *allowed; i++) {
            if (field->string && !strcmp(field->string, allowed[i]))
                found = 1;
        }
        if (!found)
            return 0;
        /* Duplicate keys make command identity/guards ambiguous. */
        for (const cJSON *prior = root->child; prior != field; prior = prior->next) {
            if (!strcmp(prior->string, field->string))
                return 0;
        }
    }
    return 1;
}

/** Read one bounded string without coercing numbers, booleans or null into identifiers. */
static int read_string(const cJSON *root, const char *key, char *out, size_t capacity,
                       int required) {
    const cJSON *value = cJSON_GetObjectItemCaseSensitive(root, key);
    if (!value)
        return required ? -1 : 0;
    if (!cJSON_IsString(value) || !value->valuestring || strlen(value->valuestring) >= capacity ||
        (required && !*value->valuestring))
        return -1;
    snprintf(out, capacity, "%s", value->valuestring);
    return 0;
}

/** Read a finite scalar; optional means absent, not null or another JSON type. */
static int read_number(const cJSON *root, const char *key, double *out, int required) {
    const cJSON *value = cJSON_GetObjectItemCaseSensitive(root, key);
    if (!value)
        return required ? -1 : 0;
    if (!cJSON_IsNumber(value) || !isfinite(value->valuedouble))
        return -1;
    *out = value->valuedouble;
    return 0;
}

/** Read an integer fencing/version counter within JSON's exact numeric range. */
static int read_counter(const cJSON *root, const char *key, uint64_t *out, int required) {
    double number = 0;
    if (!cJSON_HasObjectItem(root, key))
        return required ? -1 : 0;
    if (read_number(root, key, &number, 1) || number < 0 || number > 9007199254740991. ||
        floor(number) != number)
        return -1;
    *out = (uint64_t)number;
    return 0;
}

/** Validate version-2 wire input and convert it to one scalar actuator command. */
int bf_parse_command(const char *json, const char *machine_id, uint64_t epoch, int local,
                     BFCommand *out, char *error, size_t capacity) {
    memset(out, 0, sizeof *out);
    out->local = local;
    out->epoch = epoch;
    cJSON *root = cJSON_ParseWithOpts(json, NULL, 1);
    if (!cJSON_IsObject(root)) {
        cJSON_Delete(root);
        snprintf(error, capacity, "invalid_json_object");
        return -1;
    }
    double schema = BF_SCHEMA_VERSION;
    int bad = !known_fields(root) || read_number(root, "schema_version", &schema, !local) ||
              schema != BF_SCHEMA_VERSION;
    bad = bad || read_string(root, "command_id", out->command_id, sizeof out->command_id, !local) ||
          read_string(root, "cmd", out->cmd, sizeof out->cmd, 1) ||
          read_string(root, "machine_id", out->machine_id, sizeof out->machine_id, 0) ||
          read_string(root, "controller_boot_id", out->controller_boot_id,
                      sizeof out->controller_boot_id, 0) ||
          read_string(root, "role", out->role, sizeof out->role, 0) ||
          read_string(root, "scope", out->scope, sizeof out->scope, 0);
    if (!local)
        bad = bad || read_counter(root, "epoch", &out->epoch, 1);
    out->has_expected_version = cJSON_HasObjectItem(root, "expected_state_version");
    bad = bad || read_counter(root, "expected_state_version", &out->expected_version, 0);
    out->has_duration = cJSON_HasObjectItem(root, "duration_h");
    out->has_temperature = cJSON_HasObjectItem(root, "target_temp_c");
    bad = bad || read_number(root, "duration_h", &out->duration_h, 0) ||
          read_number(root, "target_temp_c", &out->target_temp_c, 0) ||
          read_number(root, "permit_ms", &out->permit_ms, 0) ||
          read_number(root, "sim_time_h", &out->sim_time_h, 0) ||
          read_number(root, "time_scale", &out->time_scale, 0);
    if (!*out->machine_id)
        snprintf(out->machine_id, sizeof out->machine_id, "%s", machine_id);
    if (local) {
        if (!*out->command_id)
            bf_uuid(out->command_id, sizeof out->command_id);
        if (!*out->scope)
            snprintf(out->scope, sizeof out->scope, "local");
        if (strcmp(out->cmd, "STOP") && strcmp(out->cmd, "RESUME") && strcmp(out->cmd, "FLUSH") &&
            strcmp(out->cmd, "SNAPSHOT"))
            bad = 1;
    }
    cJSON_Delete(root);
    if (bad) {
        snprintf(error, capacity, "invalid_equipment_command_v2");
        return -1;
    }
    return 0;
}

/** Read the backend's event acknowledgement; no execution state is inferred from it. */
int bf_parse_ack(const char *json, char *event_id, size_t capacity) {
    *event_id = 0;
    cJSON *root = cJSON_ParseWithOpts(json, NULL, 1);
    int invalid = !cJSON_IsObject(root) || read_string(root, "event_id", event_id, capacity, 1);
    cJSON_Delete(root);
    return invalid ? -1 : 0;
}
