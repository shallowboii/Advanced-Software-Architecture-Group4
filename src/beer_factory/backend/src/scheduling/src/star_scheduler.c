/*
 * Star scheduler. 
 *
 * Build:
 *   
 *       
 * Run:
 *   
 */

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "structure.h"
#include "redis_conn.h"

#define MAX_BATCHES      64
#define DEFAULT_BATCHES  6
#define RELEASE_INTERVAL 12   /* hours between batch releases */
#define TANK_STEP 4 //step where batch goes in tank

static void *machines[NUM_STEPS][MAX_MACHINES];  

static int was_done[NUM_STEPS][MAX_MACHINES];

typedef struct {
    int id;
    int release;              /* earliest time the batch may start step 1 */
    int start[NUM_STEPS];     /* planned start time per step */
    int machine[NUM_STEPS];   /* machine chosen per step */
    int tank;                 /* tank for this batch */
    int end;                  /* time the last step finishes */
} Batch;

/* Frees the reply and logs an error if the command failed. */
static void redis_check(redisContext *c, redisReply *r, const char *what)
{
    if (!r || r->type == REDIS_REPLY_ERROR)
        fprintf(stderr, "Redis %s failed: %s\n", what, r ? r->str : c->errstr);
    if (r) freeReplyObject(r);
}

/* Start of run: clear the old state so counters begin at zero. */
static void redis_reset(redisContext *c)
{
    redisReply *r = redisCommand(c,
        "DEL brewery:last_started brewery:last_done_type brewery:done_count brewery:batch_tank");
    redis_check(c, r, "DEL");
}

/* Call when a batch enters its tank: remembers which tank holds which batch. */
static void record_tank(redisContext *c, int batch_id, int tank)
{
    redisReply *r = redisCommand(c, "HSET brewery:batch_tank batch%d %d",
                                 batch_id, tank + 1);
    redis_check(c, r, "HSET batch_tank");
}

/* Call when a step actually starts. */
static void record_start(redisContext *c, int step, int machine,
                         int batch_id, int hour)
{
    redisReply *r = redisCommand(c,
        "HSET brewery:last_started step %d step_name %s machine %d "
        "batch %d hour %d real_time %lld",
        step + 1, PLANT[step]->name, machine + 1,
        batch_id, hour, (long long)time(NULL));
    redis_check(c, r, "HSET last_started");
}

/* Call when a machine finishes: does the "sweet_wort_batch++" part. */
static void record_done(redisContext *c, int step)
{
    const char *type = OUTPUT_TYPE[step];

    redisReply *r = redisCommand(c, "HINCRBY brewery:done_count %s 1", type);
    redis_check(c, r, "HINCRBY done_count");

    r = redisCommand(c, "SET brewery:last_done_type %s", type);
    redis_check(c, r, "SET last_done_type");
}