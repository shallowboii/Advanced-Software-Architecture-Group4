#ifndef STEPS_H
#define STEPS_H

#include "machine.h"

#define NUM_STEPS    6
#define MAX_MACHINES 8

/* PLANT[s] is the machine type for step s+1, in process order */
extern const MachineType *const PLANT[NUM_STEPS];

#endif