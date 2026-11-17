//Interface for all machines
#ifndef MACHINE_TYPE_H
#define MACHINE_TYPE_H

#include <stdbool.h>
#include <stddef.h>

typedef struct {
    const char *name;
    int count;                              /* parallel machines of this type */
    size_t size;                            /* sizeof the machine's own struct */
    void (*init)(void *self);
    int  (*duration)(const void *self);     /* the machine's own time */
    void (*add_batch)(void *self);
    bool (*start)(void *self);              /* start? */
    bool (*done)(void *self);               /* done!  */
    void (*tick)(void *self);
} MachineType;

#endif