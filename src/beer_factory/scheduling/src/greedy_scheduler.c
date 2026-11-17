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
#include "structure.h"

#define MAX_BATCHES      64
#define DEFAULT_BATCHES  6
#define RELEASE_INTERVAL 12   /* hours between batch releases */

static void *machines[NUM_STEPS][MAX_MACHINES];   

typedef struct {
    int id;
    int release;              /* earliest time the batch may start step 1 */
    int start[NUM_STEPS];     /* planned start time per step */
    int machine[NUM_STEPS];   /* machine chosen per step */
    int end;                  /* time the last step finishes */
} Batch;

static int cmp_release(const void *a, const void *b)
{
    const Batch *x = a, *y = b;
    return (x->release > y->release) - (x->release < y->release);
}

int main(int argc, char **argv)
{
    int n = (argc > 1) ? atoi(argv[1]) : DEFAULT_BATCHES;
    if (n < 1 || n > MAX_BATCHES) {
        fprintf(stderr, "batches must be 1..%d\n", MAX_BATCHES);
        return 1;
    }

    //create every machine 
    for (int s = 0; s < NUM_STEPS; s++) {
        if (PLANT[s]->count > MAX_MACHINES) {
            fprintf(stderr, "too many machines in step %d\n", s + 1);
            return 1;
        }
        for (int m = 0; m < PLANT[s]->count; m++) {
            machines[s][m] = calloc(1, PLANT[s]->size);
            if (!machines[s][m]) {
                fprintf(stderr, "out of memory\n");
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

    for (int b = 0; b < n; b++) {
        int ready = batches[b].release;

        for (int s = 0; s < NUM_STEPS; s++) {
            int best_m = 0, best_start = -1;

            for (int m = 0; m < PLANT[s]->count; m++) {
                int start = free_at[s][m] > ready ? free_at[s][m] : ready;
                if (best_start < 0 || start < best_start) {
                    best_start = start;
                    best_m = m;
                }
            }

            int finish = best_start + PLANT[s]->duration(machines[s][best_m]);
            free_at[s][best_m] = finish;
            batches[b].start[s] = best_start;
            batches[b].machine[s] = best_m;
            ready = finish;      /* next step can't start before this one ends */
        }
        batches[b].end = ready;
    }

    int makespan = 0;
    for (int b = 0; b < n; b++)
        if (batches[b].end > makespan) makespan = batches[b].end;

    //run the schedule
    for (int t = 0; t <= makespan; t++) {

        // 1. Print machine that is done
        for (int s = 0; s < NUM_STEPS; s++)
            for (int m = 0; m < PLANT[s]->count; m++)
                if (PLANT[s]->done(machines[s][m]))
                    printf("h%4d: %-16s machine %d done\n",
                           t, PLANT[s]->name, m + 1);

        // 2. start machines 
        for (int b = 0; b < n; b++)
            for (int s = 0; s < NUM_STEPS; s++)
                if (batches[b].start[s] == t) {
                    int m = batches[b].machine[s];
                    PLANT[s]->add_batch(machines[s][m]);
                    if (PLANT[s]->start(machines[s][m]))
                        printf("h%4d: %-16s machine %d start (batch %d)\n",
                               t, PLANT[s]->name, m + 1, batches[b].id);
                    else
                        printf("h%4d: ERROR %s machine %d busy (batch %d)\n",
                               t, PLANT[s]->name, m + 1, batches[b].id);
                }

        // 3. one hour passes
        for (int s = 0; s < NUM_STEPS; s++)
            for (int m = 0; m < PLANT[s]->count; m++)
                PLANT[s]->tick(machines[s][m]);
    }

    printf("\nMakespan: %d hours\n", makespan);

    for (int s = 0; s < NUM_STEPS; s++)
        for (int m = 0; m < PLANT[s]->count; m++)
            free(machines[s][m]);

    return 0;
}