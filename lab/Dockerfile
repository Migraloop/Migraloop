# Lab app image: copy a host-built migraloop binary (fast Lab bring-up).
# Use Ubuntu 24.04 so glibc matches typical developer hosts (bookworm is too old).
# Prefer `migraloop lab up`, which builds the binary when missing.
FROM ubuntu:24.04
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
ARG MIGRALOOP_BIN=target/debug/migraloop
COPY ${MIGRALOOP_BIN} /usr/local/bin/migraloop
ENTRYPOINT ["migraloop"]
CMD ["run"]
