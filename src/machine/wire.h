#ifndef BEER_WIRE_H
#define BEER_WIRE_H
#include "machine.h"
/** Validate the strict equipment-only JSON command schema and physical-input restrictions. */
int bf_parse_command(const char *json, const char *machine_id, uint64_t epoch, int local,
                     BFCommand *out, char *error, size_t cap);
/** Read one backend event acknowledgement without inferring actuator state. */
int bf_parse_ack(const char *json, char *event_id, size_t cap);
#endif
