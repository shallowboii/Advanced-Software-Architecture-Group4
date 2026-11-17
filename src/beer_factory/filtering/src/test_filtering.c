#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include "machine.h"

extern const MachineType FILTERING_TYPE;

int main(void)
{
    const MachineType *t = &FILTERING_TYPE;
    void *f = calloc(1, t->size);
    t->init(f);

    assert(t->duration(f) == 4);

    //nothing queued
    assert(!t->start(f));
    assert(!t->done(f));

    t->add_batch(f);
    assert(t->start(f));
    assert(!t->start(f));         

    // Not done at clock = 4
    for (int i = 0; i < 3; i++) {
        t->tick(f);
        assert(!t->done(f));
    }
    t->tick(f);                    
    assert(t->done(f));            
    assert(!t->done(f));           


    t->tick(f);
    t->add_batch(f);
    assert(t->start(f));
    for (int i = 0; i < 4; i++) t->tick(f);
    assert(t->done(f));

    //2 queued
    t->add_batch(f);
    t->add_batch(f);
    assert(t->start(f));
    assert(!t->start(f));          
    for (int i = 0; i < 4; i++) t->tick(f);
    assert(t->done(f));
    assert(t->start(f));           //queued one starts

    free(f);
    printf("filtering: all tests passed\n");
    return 0;
}