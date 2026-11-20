/*
 * Greedy scheduler. 
 *
 * Build:
 *   gcc -Wall -O2 -o brewery brewery_scheduler.c structure.c \
 *       mashing.c lautering.c boiling.c whirlpooling.c fermentation.c filtering.c
 * Run:
 *   ./brewery [number_of_batches]
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

static int cmp_release(const void *a, const void *b)
{
    const Batch *x = a, *y = b;
    return (x->release > y->release) - (x->release < y->release);
}

/* What each step produces when a machine finishes. Index = step. */
static const char *OUTPUT_TYPE[NUM_STEPS] = {
    "mash_batch",          /* mashing */
    "sweet_wort_batch",    /* lautering */
    "boiled_wort_batch",   /* boiling */
    "clarified_wort_batch",/* whirlpooling */
    "green_beer_batch",    /* fermentation */
    "filtered_beer_batch"  /* filtering */
};

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

int main(int argc, char **argv)
{
    int n = (argc > 1) ? atoi(argv[1]) : DEFAULT_BATCHES;
    if (n < 1 || n > MAX_BATCHES) {
        fprintf(stderr, "batches must be 1..%d\n", MAX_BATCHES);
        return 1;
    }

    /* connect to Redis */
    redisContext *rc = redis_connect();
    if (!rc) return 1;
    redis_reset(rc);

    /* create every machine */
    for (int s = 0; s < NUM_STEPS; s++) {
        if (PLANT[s]->count > MAX_MACHINES) {
            fprintf(stderr, "too many machines in step %d\n", s + 1);
            redisFree(rc);
            return 1;
        }
        for (int m = 0; m < PLANT[s]->count; m++) {
            machines[s][m] = calloc(1, PLANT[s]->size);
            if (!machines[s][m]) {
                fprintf(stderr, "out of memory\n");
                redisFree(rc);
                return 1;
            }
            PLANT[s]->init(machines[s][m]);
        }
    }

    //batches
    Batch batches[MAX_BATCHES];
    for (int i = 0; i < n; i++) {
        batches[i].id = i + 1;
        batches[i].release = i * RELEASE_INTERVAL;
    }
    qsort(batches, n, sizeof(Batch), cmp_release);

     /* ---- greedy planning ----
     * Take batches in release order. For each step, pick the machine that
     * can start the batch earliest. Each machine reports its own duration. */
    int free_at[NUM_STEPS][MAX_MACHINES] = {{0}};

    if (TANK_STEP >= NUM_STEPS) {
        fprintf(stderr, "TANK_STEP %d is out of range\n", TANK_STEP);
        redisFree(rc);
        return 1;
    }

    /* Two rules for starting a batch:
     *  1. the previous batch must already be in its tank, and
     *  2. a tank must be free: the batch reserves the tank that frees up
     *     first, and mashing does not start before that tank is free. */
    int prev_in_tank = 0;

    for (int b = 0; b < n; b++) {
        int ready = batches[b].release;
        if (b > 0 && prev_in_tank > ready)
            ready = prev_in_tank;

        /* Reserve the tank that becomes free first */
        int tank = 0;
        for (int m = 1; m < PLANT[TANK_STEP]->count; m++)
            if (free_at[TANK_STEP][m] < free_at[TANK_STEP][tank])
                tank = m;

        /* No free tank yet -> mashing waits */
        if (free_at[TANK_STEP][tank] > ready)
            ready = free_at[TANK_STEP][tank];

        for (int s = 0; s < NUM_STEPS; s++) {
            int best_m = 0, best_start = -1;

            if (s == TANK_STEP) {
                /* Use the tank reserved above */
                best_m = tank;
                best_start = free_at[s][tank] > ready ? free_at[s][tank] : ready;
            } else {
                for (int m = 0; m < PLANT[s]->count; m++) {
                    int start = free_at[s][m] > ready ? free_at[s][m] : ready;
                    if (best_start < 0 || start < best_start) {
                        best_start = start;
                        best_m = m;
                    }
                }
            }

            int finish = best_start + PLANT[s]->duration(machines[s][best_m]);
            free_at[s][best_m] = finish;
            batches[b].start[s] = best_start;
            batches[b].machine[s] = best_m;
            ready = finish;      /* next step can't start before this one ends */
        }
        batches[b].end = ready;
        /* The machine picked for TANK_STEP is the tank for this batch */
        batches[b].tank = batches[b].machine[TANK_STEP];
        prev_in_tank = batches[b].start[TANK_STEP];
    }

    printf("Tank assignment:\n");
    for (int b = 0; b < n; b++)
        printf("  batch %2d -> tank %d (enters at h%d)\n",
               batches[b].id, batches[b].tank + 1, batches[b].start[TANK_STEP]);
    printf("\n");

    int makespan = 0;
    for (int b = 0; b < n; b++)
        if (batches[b].end > makespan) makespan = batches[b].end;

    /* run the schedule */
    for (int t = 0; t <= makespan; t++) {

        /* 1. Print machines that are done, record to Redis on the rising edge */
        for (int s = 0; s < NUM_STEPS; s++)
            for (int m = 0; m < PLANT[s]->count; m++) {
                int d = PLANT[s]->done(machines[s][m]);
                if (d) {
                    printf("h%4d: %-16s machine %d done\n",
                           t, PLANT[s]->name, m + 1);
                    if (!was_done[s][m])
                        record_done(rc, s);
                }
                was_done[s][m] = d ? 1 : 0;
            }

        /* 2. Start machines */
        for (int b = 0; b < n; b++)
            for (int s = 0; s < NUM_STEPS; s++)
                if (batches[b].start[s] == t) {
                    int m = batches[b].machine[s];
                    PLANT[s]->add_batch(machines[s][m]);
                    if (PLANT[s]->start(machines[s][m])) {
                        printf("h%4d: %-16s machine %d start (batch %d)\n",
                               t, PLANT[s]->name, m + 1, batches[b].id);
                        was_done[s][m] = 0;
                        record_start(rc, s, m, batches[b].id, t);
                        if (s == TANK_STEP) {
                            printf("h%4d: batch %d is in tank %d\n",
                                   t, batches[b].id, m + 1);
                            record_tank(rc, batches[b].id, m);
                        }
                    } else {
                        printf("h%4d: ERROR %s machine %d busy (batch %d)\n",
                               t, PLANT[s]->name, m + 1, batches[b].id);
                    }
                }

        /* 3. One hour passes */
        for (int s = 0; s < NUM_STEPS; s++)
            for (int m = 0; m < PLANT[s]->count; m++)
                PLANT[s]->tick(machines[s][m]);
    }

    printf("\nMakespan: %d hours\n", makespan);

    for (int s = 0; s < NUM_STEPS; s++)
        for (int m = 0; m < PLANT[s]->count; m++)
            free(machines[s][m]);

    redisFree(rc);
    return 0;
}