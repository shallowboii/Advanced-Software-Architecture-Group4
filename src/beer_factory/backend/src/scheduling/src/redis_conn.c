#include <stdio.h>
#include <stdlib.h>
#include "redis_conn.h"

redisContext *redis_connect(void) {
    const char *host = getenv("REDIS_HOST");
    const char *port = getenv("REDIS_PORT");
    const char *user = getenv("REDIS_USERNAME");
    const char *pass = getenv("REDIS_PASSWORD");
    if (!host || !port || !pass) {
        fprintf(stderr, "Set REDIS_HOST, REDIS_PORT and REDIS_PASSWORD\n");
        return NULL;
    }

    struct timeval timeout = { 5, 0 };
    redisContext *c = redisConnectWithTimeout(host, atoi(port), timeout);
    if (!c || c->err) {
        fprintf(stderr, "Connect error: %s\n", c ? c->errstr : "alloc failed");
        if (c) redisFree(c);
        return NULL;
    }

    redisReply *r = redisCommand(c, "AUTH %s %s", user ? user : "default", pass);
    if (!r || r->type == REDIS_REPLY_ERROR) {
        fprintf(stderr, "Auth failed: %s\n", r ? r->str : c->errstr);
        if (r) freeReplyObject(r);
        redisFree(c);
        return NULL;
    }
    freeReplyObject(r);
    return c;
}