#ifndef BEER_WIRE_H
#define BEER_WIRE_H
#include "machine.h"
int bf_parse_command(const char *json, const char *machine_id, uint64_t epoch, int local,
                     BFCommand *out, char *error, size_t cap);
int bf_parse_ack(const char *json, char *event_id, size_t cap);
#endif
