#ifndef STEPS_H
#define STEPS_H

#include "filtering.h"   
//Include the other machines here

#define NUM_STEPS    6
#define MAX_MACHINES 8

typedef enum {
    STEP_MASHING,        
    STEP_LAUTERING,      
    STEP_BOILING,        
    STEP_WHIRLPOOLING,   
    STEP_FERMENTATION,   
    STEP_FILTERING       
} StepId;

//Each machine should have it's own time

#define N_FERMENTATION_TANKS 4

typedef struct {
    const char *name;
    int duration;   /* hours */
    int machines;   /* parallel machines/tanks */
} PlantStep;

static const PlantStep PLANT[NUM_STEPS] = {
    [STEP_MASHING]      = {"1 Mashing",      T_MASHING,      1},
    [STEP_LAUTERING]    = {"2 Lautering",    T_LAUTERING,    1},
    [STEP_BOILING]      = {"3 Boiling",      T_BOILING,      1},
    [STEP_WHIRLPOOLING] = {"4 Whirlpooling", T_WHIRLPOOLING, 1},
    [STEP_FERMENTATION] = {"5 Fermentation", T_FERMENTATION, N_FERMENTATION_TANKS},
    [STEP_FILTERING]    = {FILTER_NAME,      T_FILTERING,    1},
};

#endif
