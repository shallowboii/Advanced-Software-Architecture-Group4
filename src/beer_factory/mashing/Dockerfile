# =============================================================================
# Dockerfile for one machine (this copy: whirlpooling)
#
# HOW TO USE THIS FOR YOUR OWN MACHINE
#   1. Copy this file into your machine's folder, e.g. boiling/Dockerfile
#   2. Change the two ARG lines right below (MACHINE and BINARY)
#   3. Make sure your Makefile has these targets:
#        make test   -> builds and runs your tests (must exit with error if one fails)
#        make all    -> builds build/<BINARY>, the program the container runs
#      Optional: if your Makefile has a "scheduler-check" target, it is run too.
#   4. Build and run it from src/beer_factory (NOT from your machine folder):
#        cd src/beer_factory
#        docker build -f <your-folder>/Dockerfile -t <your-folder> .
#        docker run --rm <your-folder>
#
# Why build from src/beer_factory?
#   Docker can only copy files that are inside the folder you build from.
#   Machines that plug into the scheduler need scheduling/src/machine.h,
#   so we build from the parent folder that has both.
# =============================================================================

# Folder name of your machine inside src/beer_factory
ARG MACHINE=mashing
# Name of the program your "make all" builds into build/
# (boiling -> boiling-demo, fermentation_maturation -> tank-demo)
ARG BINARY=mashing


# ---- Stage 1: build and test ------------------------------------------------
# Full gcc toolchain. Big image, but it is only used to compile.
FROM gcc:14 AS build
ARG MACHINE
ARG BINARY

WORKDIR /app

# Scheduler interface (machine.h, structure.h, greedy_scheduler.c).
# Needed by machines that implement machine.h; harmless for the others.
COPY scheduling/src/ scheduling/src/

# Your machine's folder (src, tests, examples, Makefile)
COPY ${MACHINE}/ ${MACHINE}/

WORKDIR /app/${MACHINE}

# Build from scratch, run the tests, then build the program.
# If any test fails the docker build stops here, so a broken machine
# never ends up in an image.
RUN make clean 2>/dev/null || true \
 && make test \
 && if grep -q '^scheduler-check:' Makefile; then make scheduler-check; fi \
 && make all

# Same name for every machine, so the stage below doesn't need to change.
RUN cp build/${BINARY} /machine


# ---- Stage 2: the image that actually runs ---------------------------------
# Small Debian image with only the compiled program in it (no compiler).
# Same Debian version as gcc:14, so the program runs without extra setup.
FROM debian:bookworm-slim

COPY --from=build /machine /usr/local/bin/machine

# For now this runs the demo. When we have the real service (talking to the
# backend), only BINARY above has to point at that program instead.
ENTRYPOINT ["machine"]
