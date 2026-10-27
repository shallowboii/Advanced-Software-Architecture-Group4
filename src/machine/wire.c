#include "wire.h"
#include <cjson/cJSON.h>
#include <math.h>
#include <stdio.h>
#include <string.h>

static int string_field(cJSON *obj, const char *key, char *out, size_t cap, int required) {
    cJSON *v = cJSON_GetObjectItemCaseSensitive(obj, key);
    if (!v || cJSON_IsNull(v)) return required ? -1 : 0;
    if (!cJSON_IsString(v) || !v->valuestring || strlen(v->valuestring) >= cap || (required && !*v->valuestring)) return -1;
    snprintf(out, cap, "%s", v->valuestring); return 0;
}
static int number_field(cJSON *obj, const char *key, double *out, int required) {
    cJSON *v = cJSON_GetObjectItemCaseSensitive(obj, key);
    if (!v || cJSON_IsNull(v)) return required ? -1 : 0;
    if (!cJSON_IsNumber(v) || !isfinite(v->valuedouble)) return -1;
    *out = v->valuedouble; return 0;
}
int bf_parse_command(const char *json, const char *machine_id, uint64_t epoch, int local,
                     BFCommand *out, char *error, size_t cap) {
    memset(out, 0, sizeof *out); out->local = local; out->epoch = epoch;
    cJSON *root = cJSON_ParseWithOpts(json, NULL, 1);
    if (!cJSON_IsObject(root)) { cJSON_Delete(root); snprintf(error, cap, "invalid_json_object"); return -1; }
    int bad = 0; double n = 0;
    cJSON *version = cJSON_GetObjectItemCaseSensitive(root, "schema_version");
    if (version && (!cJSON_IsNumber(version) || version->valuedouble != 1)) bad = 1;
#define S(field, required) bad |= string_field(root, #field, out->field, sizeof out->field, required) != 0
    S(command_id, !local); S(cmd, 1); S(machine_id, 0); S(batch_id, 0); S(execution_id, 0);
    S(recipe_version_id, 0); S(run_id, 0); S(transfer_id, 0); S(role, 0); S(peer_id, 0);
    S(scope, 0); S(reason, 0); S(discard_reason, 0);
#undef S
    if (!*out->machine_id) snprintf(out->machine_id, sizeof out->machine_id, "%s", machine_id);
    if (local && !*out->command_id) bf_uuid(out->command_id, sizeof out->command_id);
    if (local) {
        if (strcmp(out->cmd, "STOP") && strcmp(out->cmd, "RESUME") && strcmp(out->cmd, "FLUSH") && strcmp(out->cmd, "SNAPSHOT")) bad = 1;
        if (!*out->scope) snprintf(out->scope, sizeof out->scope, "local");
    } else {
        if (number_field(root, "epoch", &n, 1) || n < 0 || n > 9007199254740991. || floor(n) != n) bad = 1;
        else out->epoch = (uint64_t)n;
    }
    if (cJSON_HasObjectItem(root, "expected_state_version")) {
        if (number_field(root, "expected_state_version", &n, 1) || n < 0 || n > 9007199254740991. || floor(n) != n) bad = 1;
        else { out->has_expected_version = 1; out->expected_version = (uint64_t)n; }
    }
#define N(field) bad |= number_field(root, #field, &out->field, 0) != 0
    N(duration_h); N(hold_h); N(max_pause_h); N(permit_ms); N(sim_time_h); N(time_scale);
#undef N
    cJSON *phases = cJSON_GetObjectItemCaseSensitive(root, "phases");
    if (phases) {
        if (!cJSON_IsArray(phases) || cJSON_GetArraySize(phases) > BF_PHASES) bad = 1;
        else {
            out->phase_count = cJSON_GetArraySize(phases);
            for (int i = 0; i < out->phase_count; i++) {
                cJSON *p = cJSON_GetArrayItem(phases, i);
                if (!cJSON_IsObject(p) || string_field(p, "name", out->phases[i].name, sizeof out->phases[i].name, 1) ||
                    number_field(p, "duration_h", &out->phases[i].duration_h, 1) ||
                    number_field(p, "target_temp_c", &out->phases[i].target_temp_c, 1)) bad = 1;
            }
        }
    }
    cJSON_Delete(root);
    if (bad) { snprintf(error, cap, "invalid_command_fields"); return -1; }
    return 0;
}
int bf_parse_ack(const char *json, char *event_id, size_t cap) {
    *event_id = 0; cJSON *root = cJSON_ParseWithOpts(json, NULL, 1);
    int rc = !cJSON_IsObject(root) || string_field(root, "event_id", event_id, cap, 1);
    cJSON_Delete(root); return rc ? -1 : 0;
}
