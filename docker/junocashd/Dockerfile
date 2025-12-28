FROM ubuntu:22.04

ARG JUNOCASH_VERSION=0.9.7

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    tar \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /tmp
RUN curl -fsSLo SHA256SUMS "https://github.com/juno-cash/junocash/releases/download/v${JUNOCASH_VERSION}/SHA256SUMS" \
 && curl -fsSLo "junocash-${JUNOCASH_VERSION}-linux64.tar.gz" "https://github.com/juno-cash/junocash/releases/download/v${JUNOCASH_VERSION}/junocash-${JUNOCASH_VERSION}-linux64.tar.gz" \
 && grep " junocash-${JUNOCASH_VERSION}-linux64.tar.gz" SHA256SUMS | sha256sum -c - \
 && tar -xzf "junocash-${JUNOCASH_VERSION}-linux64.tar.gz" \
 && install -m 0755 "junocash-${JUNOCASH_VERSION}/bin/junocashd" /usr/local/bin/junocashd \
 && install -m 0755 "junocash-${JUNOCASH_VERSION}/bin/junocash-cli" /usr/local/bin/junocash-cli \
 && rm -rf /tmp/*

VOLUME ["/data"]

ENTRYPOINT ["junocashd"]
