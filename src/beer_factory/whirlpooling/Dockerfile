# Build context must be src/beer_factory (the build needs scheduling/src):
#   cd src/beer_factory
#   docker build -f whirlpooling/Dockerfile -t whirlpooling .
#   docker run --rm whirlpooling

# ---- build stage: compile and run every test; a failing test stops the build
FROM gcc:14 AS build
WORKDIR /app
COPY scheduling/src/machine.h scheduling/src/structure.h scheduling/src/greedy_scheduler.c scheduling/src/
COPY whirlpooling/ whirlpooling/
WORKDIR /app/whirlpooling
RUN make clean && make test && make scheduler-check && make all

# ---- runtime stage: only the binary
FROM debian:bookworm-slim
COPY --from=build /app/whirlpooling/build/whirlpooling-demo /usr/local/bin/whirlpooling
ENTRYPOINT ["whirlpooling"]
