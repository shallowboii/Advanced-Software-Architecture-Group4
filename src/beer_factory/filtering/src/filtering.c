#include <stdio.h>
#include <stdbool.h>

#include "machine.h"

#define T_FILTERING 4

typedef enum { WAIT , FILTERING } State;

typedef struct {
    State state;
    int c; //clock
    int duration;
    int beer_batches; //batches to be filtered
    int finished_batches; //completely done
} Filtering;

//Setting up machine
static void init(void *self) {
    Filtering *m = self;
    m->state = WAIT;
    m->c = 0;
    m->duration = T_FILTERING;
    m->beer_batches=0;
    m->finished_batches=0;
}

static int duration(const void *self) { 
    return ((const Filtering *)self)->duration; 
}

//Add batch
static void add_batch(void *self) {
    ((Filtering *)self)->beer_batches++;
}

static bool start(void *self) {
    Filtering *m = self;
    if (m->state == WAIT && m->beer_batches>0) {
        m->c = 0;
        m->beer_batches--;
        m->state = FILTERING;
        return true;
    }
    return false;
}

static bool done(void *self) {
    Filtering *m = self;
    if (m->state == FILTERING && m->c >= m->duration) {
        m->finished_batches++;
        m->state = WAIT;
        return true;
    }
    return false;
}

//Making time pass while filtering
static void tick(void *self) {
    Filtering *m = self;
    if (m->state == FILTERING) m->c++;
}

const MachineType FILTERING_TYPE = {
    "6 Filtering", 1, sizeof(Filtering),
    init, duration, add_batch, start, done, tick
};