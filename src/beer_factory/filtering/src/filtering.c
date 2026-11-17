#include <stdio.h>
#include <stdbool.h>

#define T_FILTERING 4

typedef enum { WAIT , FILTERING } State;

typedef struct {
    State state;
    int c; //clock
    int beer_batches; //btches to be filtered
    int finished_batches; //completely done
} FilterMachine;

//Setting up machine
void init_filter(FilterMachine *m) {
    m->state = WAIT;
    m->c = 0;
    m->beer_batches=0;
    m->finished_batches=0;
}

//Add batch
void add_batch(FilterMachine *m) {
    m->beer_batches++;
}

bool start_filtering(FilterMachine *m) {
    if (m->state == WAIT && m->beer_batches>0) {
        m->c = 0;
        m->beer_batches--;
        m->state = FILTERING;
        return true;
    }
    return false;
}

bool done_filtering(FilterMachine *m) {
    if (m->state == FILTERING && m->c >= T_FILTERING) {
        finished_batches++;
        state = WAIT;
        return true;
    }
    return false;
}

//Making time pass while filtering
void tick(FilterMachine *m) {
    if (m->state == FILTERING) m->c++;
}
