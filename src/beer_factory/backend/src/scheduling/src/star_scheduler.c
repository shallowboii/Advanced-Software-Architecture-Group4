/*
 * Star scheduler. 
 *
 * Build:
 *   gcc -Wall -O2 -o brewery brewery_scheduler_uppaal.c machine_kafka.c \
 *       structure.c mashing.c lautering.c boiling.c whirlpooling.c \
 *       fermentation.c filtering.c heartbeat.c mqtt.c mqtt_pal.c \
 *       -lrdkafka -lhiredis -lpthread
 * Run:
 *   ./brewery [number_of_batches]
 *   
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
#define TANK_STEP        4   /* step where batch goes in tank (maturation) */
#define WHIRLPOOL_STEP   3   /* step that produces clarified wort */
#define FREE_TANK_MIN    4   /* start_mashing guard: free_tank >= 4 */

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
    int tank;                 /* tank for this batch, -1 until chosen */
} Batch;

typedef enum {
    ST_PENDING,   /* not sent yet */
    ST_SENT,      /* START sent, waiting for RUNNING */
    ST_RUNNING,   /* machine reported RUNNING */
    ST_DONE,      /* machine went RUNNING -> IDLE */
    ST_FAILED     /* machine reported ERROR */
} StepState;

static Batch      batches[MAX_BATCHES];
static int        n;                                /* number of batches wanted */
static int        started;                          /* batches released so far */
static StepState  st[MAX_BATCHES][NUM_STEPS];
static int        failed[MAX_BATCHES];              /* batch hit an ERROR */
static int        current[NUM_STEPS][MAX_MACHINES]; /* batch index or -1 */

/* Variables of the UPPAAL model */
static int        is_mashing;                       /* mashing in progress */
static int        tanks_status[MAX_MACHINES];       /* 0 = free, 1 = in use */
static int        clarified_wort_batches;           /* waiting for a tank */
static int        chosen_tank;                      /* last tank selected */
static time_t     tank_end[MAX_MACHINES];           /* when the tank batch is done */

static redisContext *rc;
static rd_kafka_t   *prod;
static time_t        t0;                            /* run start */

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

/* ------------------------------------------------ model helper functions */

/* How long machine m of step s runs, in real seconds. */
static double duration_s(int s, int m)
{
    return (double)PLANT[s]->duration(machines[s][m]) * SECONDS_PER_SIM_HOUR;
}

/* free_tank: how many tanks have tanks_status == 0. */
static int free_tank(void)
{
    int free_count = 0;
    for (int t = 0; t < PLANT[TANK_STEP]->count; t++)
        if (tanks_status[t] == 0) free_count++;
    return free_count;
}

/* Real seconds a new batch needs to get from mashing to a tank. */
static double lead_time_s(void)
{
    double sum = 0;
    for (int s = 0; s < TANK_STEP; s++)
        sum += duration_s(s, 0);
    return sum;
}

/* any_almost_done: some tank will be empty by the time a batch started
 * now would arrive at it. */
static int any_almost_done(void)
{
    for (int t = 0; t < PLANT[TANK_STEP]->count; t++) {
        int b = current[TANK_STEP][t];
        if (b < 0 || st[b][TANK_STEP] != ST_RUNNING) continue;
        if (difftime(tank_end[t], time(NULL)) <= lead_time_s()) return 1;
    }
    return 0;
}

/* First machine of step s that is not running anything, or -1. */
static int idle_machine(int s)
{
    for (int m = 0; m < PLANT[s]->count; m++)
        if (current[s][m] < 0) return m;
    return -1;
}

/* select t : int[0, MAX_TANKS-1] with tanks_status[t] == 0 */
static int select_free_tank(void)
{
    for (int t = 0; t < PLANT[TANK_STEP]->count; t++)
        if (tanks_status[t] == 0 && current[TANK_STEP][t] < 0) return t;
    return -1;
}

/* ------------------------------------------------------------ commands */

/* start_X!: send START for step s of batch b to machine m. */
static void send_start(int b, int s, int m)
{
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
    msg.params[0].value = duration_s(s, m);
    msg.n_params = 1;

    snprintf(topic, sizeof topic, "machine.%s", PLANT[s]->name);
    mk_send(prod, topic, &msg);

    st[b][s] = ST_SENT;
    current[s][m] = b;
    printf("h%4d: %-16s machine %d START sent (batch %d)\n",
           sim_hour(), PLANT[s]->name, m + 1, batches[b].id);
}

/* Wait -> Mash -> Wait, with start_mashing! */
static void edge_start_mashing(void)
{
    int m = idle_machine(0);
    if (m < 0) return;

    int b = started++;
    batches[b].id = b + 1;
    batches[b].tank = -1;
    is_mashing = 1;
    send_start(b, 0, m);
}

/* Guard of the Wait -> Mash edge. A new batch is only released while fewer
 * than n have started. */
static int can_mash(void)
{
    return started < n && !is_mashing &&
           (free_tank() >= FREE_TANK_MIN || any_almost_done());
}

/* Batch b sits in the committed location of step s (Lauter, Boil, Whirlpool,
 * Maturation or Filter): the previous step is done, this one is not sent. */
static int in_location(int b, int s)
{
    return !failed[b] && st[b][s - 1] == ST_DONE && st[b][s] == ST_PENDING;
}

/* Committed location -> Wait, with start_X!. If the edge cannot fire yet
 * (no idle machine, no free tank) the batch stays in its location and the
 * edge is tried again later. The scheduler does not block meanwhile, since
 * the done_X? that frees the machine or tank must still be received. */
static void edge_start(int b, int s)
{
    int m;

    if (s == TANK_STEP) {
        /* guard: tanks_status[t] == 0 && clarified_wort_batches >= 1 */
        if (clarified_wort_batches < 1) return;
        m = select_free_tank();
        if (m < 0) return;
        chosen_tank = m;
        tanks_status[m] = 1;
        clarified_wort_batches--;
        batches[b].tank = m;
        printf("h%4d: batch %d goes to tank %d\n",
               sim_hour(), batches[b].id, chosen_tank + 1);
    } else {
        m = idle_machine(s);
        if (m < 0) return;
    }
    send_start(b, s, m);
}

/* Fire every edge that is enabled right now. */
static void fire_edges(void)
{
    /* committed locations first, oldest batch first */
    for (int b = 0; b < started; b++)
        for (int s = 1; s < NUM_STEPS; s++)
            if (in_location(b, s))
                edge_start(b, s);

    if (can_mash())
        edge_start_mashing();
}

/* -------------------------------------------------------------- status */

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
            tank_end[m] = time(NULL) + (time_t)duration_s(s, m);
            printf("h%4d: batch %d is in tank %d\n",
                   hour, batches[b].id, m + 1);
            record_tank(rc, batches[b].id, m);
        }
        break;

    case MK_IDLE:
        /* done_X?: the batch is now in the next committed location */
        if (st[b][s] != ST_RUNNING) break;           /* IDLE before RUNNING, or dup */
        st[b][s] = ST_DONE;
        current[s][m] = -1;
        printf("h%4d: %-16s machine %d done (batch %d)\n",
               hour, PLANT[s]->name, m + 1, batches[b].id);
        record_done(rc, s);
        if (s == 0)
            is_mashing = 0;
        if (s == WHIRLPOOL_STEP)
            clarified_wort_batches++;
        if (s == TANK_STEP) {
            tanks_status[m] = 0;                     /* tank is free again */
            printf("h%4d: tank %d is free\n", hour, m + 1);
        }
        if (s == NUM_STEPS - 1)
            printf("h%4d: batch %d is finished\n", hour, batches[b].id);
        break;

    case MK_ERROR:
        st[b][s] = ST_FAILED;
        failed[b] = 1;
        current[s][m] = -1;
        if (s == 0)
            is_mashing = 0;
        if (s == TANK_STEP)
            tanks_status[m] = 0;   /* assumes a failed batch leaves its tank empty */
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

/* Every batch has been released and is either finished or failed (a failed
 * machine is freed on ERROR, so nothing is left in flight for a failed batch). */
static int all_done(void)
{
    if (started < n) return 0;
    for (int b = 0; b < n; b++)
        if (!failed[b] && st[b][NUM_STEPS - 1] != ST_DONE)
            return 0;
    return 1;
}

/* ---------------------------------------------------------------- main */

int main(int argc, char **argv)
{
    n = (argc > 1) ? atoi(argv[1]) : DEFAULT_BATCHES;
    if (n < 1 || n > MAX_BATCHES) {
        fprintf(stderr, "batches must be 1..%d\n", MAX_BATCHES);
        return 1;
    }
    if (NUM_STEPS != 6 || TANK_STEP != 4 || WHIRLPOOL_STEP != 3) {
        fprintf(stderr, "the UPPAAL model has 6 steps (tank step 4)\n");
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

    /* With fewer tanks than FREE_TANK_MIN and no batch in a tank, the
     * start_mashing guard could never become true. */
    if (PLANT[TANK_STEP]->count < FREE_TANK_MIN) {
        fprintf(stderr, "plant has %d tanks, start_mashing needs free_tank >= %d\n",
                PLANT[TANK_STEP]->count, FREE_TANK_MIN);
        redisFree(rc);
        return 1;
    }

    /* ---- run-time state ---- */
    for (int s = 0; s < NUM_STEPS; s++)
        for (int m = 0; m < MAX_MACHINES; m++)
            current[s][m] = -1;
    /* st[][], failed[], tanks_status[] and the model variables are static,
     * so already zero == ST_PENDING / not failed / tank free / Wait */

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

    /* Heartbeats are monitoring only: if MQTT is down, run without them. */
    if (hb_start() < 0)
        fprintf(stderr, "MQTT unavailable, running without heartbeats\n");

    /* Joining a consumer group takes a few seconds. Poll before the first
     * dispatch so an early RUNNING reply is not missed. current[][] is all
     * -1 here, so on_status ignores everything received during warm-up. */
    time_t warm = time(NULL);
    while (time(NULL) - warm < WARMUP_S) {
        mk_poll(cons, on_status, NULL);
        hb_poll();
    }

    /* ---- run the automaton, driven by machine statuses ---- */
    t0 = time(NULL);
    int timed_out = 0;

    while (!all_done()) {
        if (time(NULL) - t0 >= RUN_TIMEOUT_S) { timed_out = 1; break; }

        /* 1. Fire every edge that is enabled */
        fire_edges();

        /* 2. Wait for a status message (blocks up to MK_POLL_TIMEOUT_MS) */
        mk_poll(cons, on_status, NULL);
        hb_poll();
    }

    int nfailed = 0;
    for (int b = 0; b < started; b++) nfailed += failed[b];

    if (timed_out)
        printf("\nTIMEOUT after %d s, some machines never finished\n",
               RUN_TIMEOUT_S);
    printf("\nActual: %d hours, batches started: %d, failed batches: %d\n",
           sim_hour(), started, nfailed);

    hb_stop(1);
    mk_consumer_close(cons);
    mk_producer_close(prod);

    for (int s = 0; s < NUM_STEPS; s++)
        for (int m = 0; m < PLANT[s]->count; m++)
            free(machines[s][m]);

    redisFree(rc);
    return (timed_out || nfailed) ? 1 : 0;
}