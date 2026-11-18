# Whirlpooling

This is the whirlpooling machine in C, made from the Whirlpooling template in the UPPAAL model (`src/docs/Beer_Factory.xml`).

## What I tried to do

We had two ways of writing a machine:

- **boiling** - a state machine that follows the UPPAAL model, works in seconds and returns result codes
- **filtering** - implements `machine.h` so the scheduler can use it

I basically combined the two, so the machine follows the model and the scheduler can still work with it easily.

- `whirlpooling.c` / `whirlpooling.h` are done the same way as boiling. This is the actual machine: it waits for hot wort, takes one batch, whirlpools it for 1 hour, then makes one clarified wort batch and goes back to waiting. Same states, guards and updates as in UPPAAL.
- `whirlpooling_machine.c` implements `machine.h` (from `scheduling/src`), like filtering does. It doesn't have its own logic, it just calls the functions from `whirlpooling.c`. One scheduler tick is one hour, so it adds 3600 seconds each tick.

This way the machine logic is in one place and matches the model, and the scheduler can still use it without changing anything in `scheduling/`.

## Files

| File | What it is |
| --- | --- |
| `src/whirlpooling.c`, `src/whirlpooling.h` | the machine (boiling style) |
| `src/whirlpooling_machine.c` | the part the scheduler uses (`WHIRLPOOLING_TYPE`) |
| `tests/test_whirlpooling.c` | tests for the machine |
| `tests/test_whirlpooling_machine.c` | tests it the way the scheduler calls it |
| `tests/scheduler_plant.c` | runs the real scheduler with my machine at step 4 (the other steps are just stand-ins for now) |
| `examples/demo.c` | runs one batch and prints what happens |

## How to run it

From this folder in Git Bash:

```bash
make test             # all the tests
make scheduler-check  # runs the real scheduler for 6 batches, fails if anything goes wrong
make demo             # one batch, step by step
make clean            # deletes build/
```

With Docker, from `src/beer_factory` (the build needs files from `scheduling/` too):

```bash
docker build -f whirlpooling/Dockerfile -t whirlpooling .
docker run --rm whirlpooling
```

The Docker build runs the tests first, so if they fail the image isn't built.

The Dockerfile is also meant as a template for the other machines: copy it to your folder, change `MACHINE` and `BINARY` at the top, and you're done (the comments in it explain the rest).

## Differences from boiling

- **No output limit.** Boiling won't start if its output is full. The UPPAAL whirlpooling template doesn't have that guard (`MAX_WHIRLPOOLED_BATCHES` is there but never used), so I left it out too.
- **No error while waiting.** Calling update while the machine is waiting just returns "no change" instead of an error, because the scheduler checks every machine every hour, even the idle ones.
