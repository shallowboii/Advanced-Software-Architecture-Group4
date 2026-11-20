#ifndef REDIS_CONN_H
#define REDIS_CONN_H

#include <hiredis/hiredis.h>

// Returns a connected, authenticated context, or NULL on failure.
redisContext *redis_connect(void);

#endif