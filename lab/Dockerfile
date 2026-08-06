# Lab app image: copy a host-built migraloop binary (fast Lab bring-up).
# Use Ubuntu 24.04 so glibc matches typical developer hosts (bookworm is too old).
# Prefer `migraloop lab up`, which builds the binary when missing.
#
# Instant Client Basic Light is required so Fixture `migraloop run` can open
# Oracle LogMiner (OCI) for continuous Incremental Capture — same dependency as
# host Scenario apply/sync (`LD_LIBRARY_PATH`).
FROM ubuntu:24.04
ARG OIC_VERSION=23.9.0.25.07
ARG OIC_ZIP_DIR=2390000
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        unzip \
        libaio1t64 \
        libnsl2 \
    && ln -sf /usr/lib/x86_64-linux-gnu/libaio.so.1t64 /usr/lib/x86_64-linux-gnu/libaio.so.1 \
    && mkdir -p /opt/oracle \
    && curl -fsSLo /tmp/instantclient-basiclite.zip \
        "https://download.oracle.com/otn_software/linux/instantclient/${OIC_ZIP_DIR}/instantclient-basiclite-linux.x64-${OIC_VERSION}.zip" \
    && unzip -q /tmp/instantclient-basiclite.zip -d /opt/oracle \
    && rm -f /tmp/instantclient-basiclite.zip \
    && apt-get purge -y curl unzip \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*
ENV LD_LIBRARY_PATH=/opt/oracle/instantclient_23_9
ARG MIGRALOOP_BIN=target/debug/migraloop
COPY ${MIGRALOOP_BIN} /usr/local/bin/migraloop
ENTRYPOINT ["migraloop"]
CMD ["run"]
