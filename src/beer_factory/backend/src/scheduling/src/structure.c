#include "structure.h"

extern const MachineType MASHING_TYPE;
extern const MachineType LAUTERING_TYPE;
extern const MachineType BOILING_TYPE;
extern const MachineType WHIRLPOOLING_TYPE;
extern const MachineType FERMENTATION_TYPE;
extern const MachineType FILTERING_TYPE;

const MachineType *const PLANT[NUM_STEPS] = {
    &MASHING_TYPE,        
    &LAUTERING_TYPE,      
    &BOILING_TYPE,        
    &WHIRLPOOLING_TYPE,   
    &FERMENTATION_TYPE,   
    &FILTERING_TYPE,      
};