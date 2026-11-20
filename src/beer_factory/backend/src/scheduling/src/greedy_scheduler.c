/*
 * Greedy scheduler. 
 *
 * Build:
 *   gcc -Wall -O2 -o brewery brewery_scheduler.c machine_kafka.c structure.c \
 *       mashing.c lautering.c boiling.c whirlpooling.c fermentation.c \
 *       filtering.c heartbeat.c mqtt.c mqtt_pal.c \
 *       -lrdkafka -lhiredis -lpthread
 * Run:
 *   ./brewery [number_of_batches]
 */
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <netdb.h>
#include <sys/socket.h>
#include "structure.h"
#include "redis_conn.h"
#include "machine_kafka.h"
#include "heartbeat.h"

#define MAX_BATCHES      64
#define DEFAULT_BATCHES  6
#define RELEASE_INTERVAL 0   /* hours between batch releases */
#define TANK_STEP 4 //step where batch goes in tank

//Interact with kafka
#define BROKERS              "localhost:9092"
#define CONSUMER_GROUP       "brewery-scheduler"
#define STATUS_TOPIC_REGEX   "^machine\\..+"  /* every machine type's topic */
#define SECONDS_PER_SIM_HOUR 1    /* time scale: 1 sim hour = 1 real second */
#define WARMUP_S             5    /* let the consumer join its group first */
#define RUN_TIMEOUT_S        3600 /* give up if machines never finish */

//Heartbeats from the machines over MQTT
#define MQTT_HOST            "localhost"
#define MQTT_PORT            "1883"
#define MQTT_CLIENT_ID       "brewery-scheduler-hb"
#define HEARTBEAT_TOPIC      "heartbeat"  /* same topic heartbeat_send uses */
#define HEARTBEAT_TIMEOUT_S  30   /* machine is "silent" after this long */
#define MAX_HEARTBEATS       64   /* distinct machine names we can track */
#define MQTT_BUF_SIZE        2048

static void *machines[NUM_STEPS][MAX_MACHINES];  

typedef struct {
    int id;
    int release;              /* earliest time the batch may start step 1 */
    int start[NUM_STEPS];     /* planned start time per step */
    int machine[NUM_STEPS];   /* machine chosen per step */
    int tank;                 /* tank for this batch */
    int end;                  /* time the last step finishes */
} Batch;

typedef enum {
    ST_PENDING,   /* not sent yet */
    ST_SENT,      /* START sent, waiting for RUNNING */
    ST_RUNNING,   /* machine reported RUNNING */
    ST_DONE,      /* machine went RUNNING -> IDLE */
    ST_FAILED     /* machine reported ERROR */
} StepState;

static Batch      batches[MAX_BATCHES];
static int        n;                                /* number of batches */
static StepState  st[MAX_BATCHES][NUM_STEPS];
static int        failed[MAX_BATCHES];              /* batch hit an ERROR */
static int        current[NUM_STEPS][MAX_MACHINES]; /* batch index or -1 */
static int        tank_owner[MAX_MACHINES];         /* batch index or -1 */

static redisContext *rc;
static rd_kafka_t   *prod;
static time_t        t0;                            /* run start */

static int cmp_release(const void *a, const void *b)
{
    const Batch *x = a, *y = b;
    if (x->release != y->release)
        return (x->release > y->release) - (x->release < y->release);
    return (x->id > y->id) - (x->id < y->id);   /* qsort is not stable */
}

/* What each step produces when a machine finishes. */
static const char *OUTPUT_TYPE[NUM_STEPS] = {
    "mash_batch",          
    "sweet_wort_batch",    
    "boiled_wort_batch",   
    "clarified_wort_batch",
    "green_beer_batch",    
    "filtered_beer_batch"  
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
        "DEL brewery:last_started brewery:last_done_type brewery:done_count "
        "brewery:batch_tank brewery:last_error brewery:heartbeat");
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

/* Call when a machine reports ERROR. */
static void record_error(redisContext *c, int step, int machine, int batch_id,
                         const char *code, const char *reason)
{
    redisReply *r = redisCommand(c,
        "HSET brewery:last_error step %d step_name %s machine %d batch %d "
        "code %s reason %s real_time %lld",
        step + 1, PLANT[step]->name, machine + 1, batch_id,
        code[0] ? code : "-", reason[0] ? reason : "-",
        (long long)time(NULL));
    redis_check(c, r, "HSET last_error");
}

static int sim_hour(void)
{
    return (int)((time(NULL) - t0) / SECONDS_PER_SIM_HOUR);
}

static void utc_now(char *buf, size_t len)
{
    time_t t = time(NULL);
    struct tm tm;
    gmtime_r(&t, &tm);
    strftime(buf, len, "%Y-%m-%dT%H:%M:%SZ", &tm);
}

/* Last heartbeat seen from each machine, keyed by the name it sends. */
typedef struct {
    char   name[MAX_MACHINE_NAME_LEN];
    time_t last_seen;         /* local receive time, not the machine's clock */
    int    silent;            /* already reported as silent */
} HbEntry;

static HbEntry            hb_tab[MAX_HEARTBEATS];
static int                hb_n;
static struct mqtt_client hb_client;
static int                hb_fd = -1;                /* -1 = MQTT not running */
static uint8_t            hb_send_buf[MQTT_BUF_SIZE];
static uint8_t            hb_recv_buf[MQTT_BUF_SIZE];

/* Called by mqtt_sync for every message on the heartbeat topic. */
static void on_heartbeat(void **state, struct mqtt_response_publish *p)
{
    (void)state;

    /* The sender publishes the raw struct, so the size must match exactly. */
    if (p->application_message_size != sizeof(machine_heartbeat_t)) {
        fprintf(stderr, "MQTT: heartbeat of %zu bytes ignored (expected %zu)\n",
                p->application_message_size, sizeof(machine_heartbeat_t));
        return;
    }

    machine_heartbeat_t hb;
    memcpy(&hb, p->application_message, sizeof hb);
    hb.machine_name[MAX_MACHINE_NAME_LEN - 1] = '\0';

    HbEntry *e = NULL;
    for (int i = 0; i < hb_n; i++)
        if (!strcmp(hb_tab[i].name, hb.machine_name)) { e = &hb_tab[i]; break; }

    if (!e) {
        if (hb_n >= MAX_HEARTBEATS) return;
        e = &hb_tab[hb_n++];
        snprintf(e->name, sizeof e->name, "%s", hb.machine_name);
        printf("h%4d: heartbeat: first one from %s\n", sim_hour(), e->name);
    } else if (e->silent) {
        printf("h%4d: heartbeat: %s is back\n", sim_hour(), e->name);
    }
    e->last_seen = time(NULL);
    e->silent = 0;

    redisReply *r = redisCommand(rc, "HSET brewery:heartbeat %s %lld",
                                 e->name, (long long)e->last_seen);
    redis_check(rc, r, "HSET heartbeat");
}

/* Connected, non-blocking TCP socket to host:port, or -1. */
static int open_nonblocking_socket(const char *host, const char *port)
{
    struct addrinfo hints = {0}, *res, *ai;
    int fd = -1;

    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    if (getaddrinfo(host, port, &hints, &res) != 0) return -1;

    for (ai = res; ai; ai = ai->ai_next) {
        fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (fd < 0) continue;
        if (connect(fd, ai->ai_addr, ai->ai_addrlen) == 0) break;
        close(fd);
        fd = -1;
    }
    freeaddrinfo(res);

    if (fd >= 0) fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK);
    return fd;
}

static void hb_stop(int graceful)
{
    if (hb_fd < 0) return;
    if (graceful) {
        mqtt_disconnect(&hb_client);
        mqtt_sync(&hb_client);                 /* push the DISCONNECT out */
    }
    close(hb_fd);
    hb_fd = -1;
}

/* Connect to the broker and subscribe. Returns 0 on success, -1 otherwise. */
static int hb_start(void)
{
    hb_fd = open_nonblocking_socket(MQTT_HOST, MQTT_PORT);
    if (hb_fd < 0) return -1;

    enum MQTTErrors e = mqtt_init(&hb_client, hb_fd,
                                  hb_send_buf, sizeof hb_send_buf,
                                  hb_recv_buf, sizeof hb_recv_buf,
                                  on_heartbeat);
    if (e == MQTT_OK)
        e = mqtt_connect(&hb_client, MQTT_CLIENT_ID, NULL, NULL, 0, NULL, NULL,
                         MQTT_CONNECT_CLEAN_SESSION, 60);
    if (e == MQTT_OK)
        e = mqtt_subscribe(&hb_client, HEARTBEAT_TOPIC, 0);
    if (e != MQTT_OK) {
        fprintf(stderr, "MQTT: %s\n", mqtt_error_str(e));
        hb_stop(0);
        return -1;
    }
    return 0;
}

/* Non-blocking: send/receive pending MQTT traffic and flag silent machines.
 * Call often (every main loop iteration). */
static void hb_poll(void)
{
    if (hb_fd < 0) return;

    enum MQTTErrors e = mqtt_sync(&hb_client);
    if (e != MQTT_OK) {
        fprintf(stderr, "MQTT: %s, heartbeats disabled\n", mqtt_error_str(e));
        hb_stop(0);
        return;
    }

    time_t now = time(NULL);
    for (int i = 0; i < hb_n; i++)
        if (!hb_tab[i].silent && now - hb_tab[i].last_seen > HEARTBEAT_TIMEOUT_S) {
            hb_tab[i].silent = 1;
            fprintf(stderr, "h%4d: heartbeat: %s silent for over %d s\n",
                    sim_hour(), hb_tab[i].name, HEARTBEAT_TIMEOUT_S);
        }
}

/* A tank with no batch reserved on it, or -1 when every tank is taken. */
static int free_tank(void)
{
    for (int m = 0; m < PLANT[TANK_STEP]->count; m++)
        if (tank_owner[m] < 0) return m;
    return -1;
}

static void release_tanks_of(int b)
{
    for (int m = 0; m < MAX_MACHINES; m++)
        if (tank_owner[m] == b) tank_owner[m] = -1;
}

/* The nearest earlier batch that has not failed, or -1. */
static int prev_active(int b)
{
    for (int p = b - 1; p >= 0; p--)
        if (!failed[p]) return p;
    return -1;
}

/* May batch b start step s right now? Same rules as the old planner, but
 * checked against what the machines actually reported. */
static int can_start(int b, int s)
{
    int m = batches[b].machine[s];

    if (failed[b]) return 0;
    if (st[b][s] != ST_PENDING) return 0;
    if (s > 0 && st[b][s - 1] != ST_DONE) return 0;      /* previous step */

    if (s == 0) {
        /* Rule 1: the previous batch must already be in its tank. */
        int p = prev_active(b);
        if (p >= 0) {
            StepState ps = st[p][TANK_STEP];
            if (ps != ST_RUNNING && ps != ST_DONE) return 0;
        }
        /* Rule 2: a tank must be free right now. */
        if (free_tank() < 0) return 0;
    }

    if (current[s][m] >= 0) return 0;                    /* machine busy */

    /* keep the planned order on each machine (not for the tank step:
     * tanks are reserved exclusively, so there is no queue) */
    if (s != TANK_STEP)
        for (int p = 0; p < b; p++) {
            if (failed[p]) continue;
            if (batches[p].machine[s] == m && st[p][s] != ST_DONE) return 0;
        }
    return 1;
}

static void send_start(int b, int s)
{
    if (s == 0) {
        /* Reserve a free tank for this batch (can_start checked there is one) */
        int t = free_tank();
        tank_owner[t] = b;
        batches[b].tank = t;
        batches[b].machine[TANK_STEP] = t;
        printf("h%4d: batch %d reserves tank %d\n",
               sim_hour(), batches[b].id, t + 1);
    }

    int m = batches[b].machine[s];
    mk_message_t msg = {0};
    char topic[64];

    msg.type = MK_START;
    snprintf(msg.machine_type, sizeof msg.machine_type, "%s", PLANT[s]->name);
    msg.machine_id = m + 1;
    snprintf(msg.batch_id, sizeof msg.batch_id, "batch%d", batches[b].id);
    snprintf(msg.message_id, sizeof msg.message_id,
             "batch%d-step%d-start", batches[b].id, s + 1);   /* stable id */
    utc_now(msg.ts_utc, sizeof msg.ts_utc);

    snprintf(msg.params[0].name, sizeof msg.params[0].name, "duration_s");
    msg.params[0].value =
        (double)PLANT[s]->duration(machines[s][m]) * SECONDS_PER_SIM_HOUR;
    msg.n_params = 1;

    snprintf(topic, sizeof topic, "machine.%s", PLANT[s]->name);
    mk_send(prod, topic, &msg);

    st[b][s] = ST_SENT;
    current[s][m] = b;
    printf("h%4d: %-16s machine %d START sent (batch %d)\n",
           sim_hour(), PLANT[s]->name, m + 1, batches[b].id);
}

static int step_of(const char *type)
{
    for (int s = 0; s < NUM_STEPS; s++)
        if (!strcmp(PLANT[s]->name, type)) return s;
    return -1;
}

/* Called by mk_poll for every message on the machine.* topics. */
static void on_status(const mk_message_t *msg, void *opaque)
{
    (void)opaque;

    /* commands and statuses share a topic: ignore our own commands */
    if (msg->type != MK_IDLE && msg->type != MK_RUNNING &&
        msg->type != MK_ERROR)
        return;

    int s = step_of(msg->machine_type);
    int m = msg->machine_id - 1;
    if (s < 0 || m < 0 || m >= PLANT[s]->count) return;

    int b = current[s][m];
    if (b < 0) return;        /* idle heartbeat, or not something we started */

    /* If the status names a batch, it must be the one we expect on this
     * machine. This stops a redelivered status of an earlier batch from being
     * credited to the batch that now uses the same machine. */
    char expect[MK_BATCH_LEN];
    snprintf(expect, sizeof expect, "batch%d", batches[b].id);
    if (msg->batch_id[0] && strcmp(msg->batch_id, expect) != 0) return;

    int hour = sim_hour();

    switch (msg->type) {
    case MK_RUNNING:
        if (st[b][s] != ST_SENT) break;              /* duplicate */
        st[b][s] = ST_RUNNING;
        printf("h%4d: %-16s machine %d running (batch %d)\n",
               hour, PLANT[s]->name, m + 1, batches[b].id);
        record_start(rc, s, m, batches[b].id, hour);
        if (s == TANK_STEP) {
            printf("h%4d: batch %d is in tank %d\n",
                   hour, batches[b].id, m + 1);
            record_tank(rc, batches[b].id, m);
        }
        break;

    case MK_IDLE:
        if (st[b][s] != ST_RUNNING) break;           /* IDLE before RUNNING, or dup */
        st[b][s] = ST_DONE;
        current[s][m] = -1;
        printf("h%4d: %-16s machine %d done (batch %d)\n",
               hour, PLANT[s]->name, m + 1, batches[b].id);
        record_done(rc, s);
        if (s == TANK_STEP) {
            tank_owner[m] = -1;                      /* tank is free again */
            printf("h%4d: tank %d is free\n", hour, m + 1);
        }
        break;

    case MK_ERROR:
        st[b][s] = ST_FAILED;
        failed[b] = 1;
        current[s][m] = -1;
        release_tanks_of(b);   /* assumes a failed batch leaves its tank empty */
        fprintf(stderr, "h%4d: ERROR %s machine %d (batch %d): %s %s\n",
                hour, PLANT[s]->name, m + 1, batches[b].id,
                msg->error_code, msg->error_reason);
        record_error(rc, s, m, batches[b].id,
                     msg->error_code, msg->error_reason);
        break;

    default:
        break;
    }
}

/* Every batch is either finished or failed (a failed machine is freed on
 * ERROR, so nothing is left in flight for a failed batch). */
static int all_done(void)
{
    for (int b = 0; b < n; b++)
        if (!failed[b] && st[b][NUM_STEPS - 1] != ST_DONE)
            return 0;
    return 1;
}

int main(int argc, char **argv)
{
    n = (argc > 1) ? atoi(argv[1]) : DEFAULT_BATCHES;
    if (n < 1 || n > MAX_BATCHES) {
        fprintf(stderr, "batches must be 1..%d\n", MAX_BATCHES);
        return 1;
    }
    if (TANK_STEP < 1 || TANK_STEP >= NUM_STEPS) {
        fprintf(stderr, "TANK_STEP %d is out of range\n", TANK_STEP);
        return 1;
    }

    /* connect to Redis */
    rc = redis_connect();
    if (!rc) return 1;
    redis_reset(rc);

    /* Local machine objects are only used to ask PLANT[s]->duration().
     * They are never started or ticked: the real machines do that. */
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

    // batches
    for (int i = 0; i < n; i++) {
        batches[i].id = i + 1;
        batches[i].release = i * RELEASE_INTERVAL;
    }
    qsort(batches, n, sizeof(Batch), cmp_release);

    /* ---- greedy planning ----
     * Take batches in release order. For each step, pick the machine that
     * can start the batch earliest. Each machine reports its own duration.
     * The result is used for machine assignment and ordering only; real
     * start times come from the machines' status messages. */
    int free_at[NUM_STEPS][MAX_MACHINES] = {{0}};

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

    printf("Planned tank assignment (real tanks are picked at run time):\n");
    for (int b = 0; b < n; b++)
        printf("  batch %2d -> tank %d (planned entry h%d)\n",
               batches[b].id, batches[b].tank + 1, batches[b].start[TANK_STEP]);
    printf("\n");

    int planned_makespan = 0;
    for (int b = 0; b < n; b++)
        if (batches[b].end > planned_makespan) planned_makespan = batches[b].end;

    /* ---- run-time state ---- */
    for (int s = 0; s < NUM_STEPS; s++)
        for (int m = 0; m < MAX_MACHINES; m++)
            current[s][m] = -1;
    for (int m = 0; m < MAX_MACHINES; m++)
        tank_owner[m] = -1;
    /* st[][] and failed[] are static, so already zero == ST_PENDING / 0 */

    /* ---- Kafka ---- */
    prod = mk_producer_new(BROKERS);
    rd_kafka_t *cons = mk_consumer_new(BROKERS, CONSUMER_GROUP,
                                       STATUS_TOPIC_REGEX);
    if (!prod || !cons) {
        fprintf(stderr, "Kafka setup failed\n");
        if (prod) mk_producer_close(prod);
        if (cons) mk_consumer_close(cons);
        redisFree(rc);
        return 1;
    }

    /* Joining a consumer group takes a few seconds. Poll before the first
     * dispatch so an early RUNNING reply is not missed. current[][] is all
     * -1 here, so on_status ignores everything received during warm-up. */
    /* Heartbeats are monitoring only: if MQTT is down, run without them. */
    if (hb_start() < 0)
        fprintf(stderr, "MQTT unavailable, running without heartbeats\n");

    time_t warm = time(NULL);
    while (time(NULL) - warm < WARMUP_S) {
        mk_poll(cons, on_status, NULL);
        hb_poll();
    }

    /* ---- run the schedule, driven by machine statuses ---- */
    t0 = time(NULL);
    int timed_out = 0;

    while (!all_done()) {
        if (time(NULL) - t0 >= RUN_TIMEOUT_S) { timed_out = 1; break; }

        /* 1. Start everything that is allowed to start */
        for (int b = 0; b < n; b++)
            for (int s = 0; s < NUM_STEPS; s++)
                if (can_start(b, s))
                    send_start(b, s);

        /* 2. Wait for a status message (blocks up to MK_POLL_TIMEOUT_MS) */
        mk_poll(cons, on_status, NULL);
        hb_poll();
    }

    int nfailed = 0;
    for (int b = 0; b < n; b++) nfailed += failed[b];

    if (timed_out)
        printf("\nTIMEOUT after %d s, some machines never finished\n",
               RUN_TIMEOUT_S);
    printf("\nPlanned makespan: %d hours, actual: %d hours, failed batches: %d\n",
           planned_makespan, sim_hour(), nfailed);

    hb_stop(1);
    mk_consumer_close(cons);
    mk_producer_close(prod);

    for (int s = 0; s < NUM_STEPS; s++)
        for (int m = 0; m < PLANT[s]->count; m++)
            free(machines[s][m]);

    redisFree(rc);
    return (timed_out || nfailed) ? 1 : 0;
}
