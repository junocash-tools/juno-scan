FROM ubuntu:22.04 AS builder

ARG JUNOCASH_REPO=https://github.com/juno-cash/junocash.git
ARG JUNOCASH_COMMIT=eaaeb5a85

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    git \
    build-essential \
    pkg-config \
    python3 \
    automake \
    autoconf \
    libtool \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /src
RUN git clone --depth 1 ${JUNOCASH_REPO} .
RUN git fetch --depth 1 origin ${JUNOCASH_COMMIT} && git checkout ${JUNOCASH_COMMIT}

RUN ./zcutil/build.sh -j"$(nproc)"

FROM ubuntu:22.04

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
  && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/src/junocashd /usr/local/bin/junocashd
COPY --from=builder /src/src/junocash-cli /usr/local/bin/junocash-cli

VOLUME ["/data"]

ENTRYPOINT ["junocashd"]

