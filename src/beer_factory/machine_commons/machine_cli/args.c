#include "args.h"
#include <stdio.h>
#include <stdlib.h>

int get_machine_number(int argc, char **argv, long *machine_number) {
  if (argc != 2) {
    fprintf(stderr, "Usage: %s <machine-number>\n", argv[0]);
    return 0;
  }

  long number = atoi(argv[1]);
  if (number < 0) {
    printf("Machine number must be >= 0\n");
    return 0;
  }

  *machine_number = number;
  return 1;
}
