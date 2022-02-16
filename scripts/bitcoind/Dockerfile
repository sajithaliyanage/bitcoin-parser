FROM python:3.10

LABEL maintainer.0="CIBR-QCRI Team"

RUN useradd -r bitcoin \
  && apt-get update -y \
  && apt-get install -y curl gnupg gosu \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* 

ENV BITCOIN_VERSION=0.18.1
ENV BITCOIN_DATA=/mnt/data/bitcoin
ENV PATH=/opt/bitcoin-${BITCOIN_VERSION}/bin:$PATH
ENV BITCOIN_DAEMON_HOST=127.0.0.1

RUN set -ex \
  && curl -SLO https://bitcoin.org/bin/bitcoin-core-${BITCOIN_VERSION}/bitcoin-${BITCOIN_VERSION}-x86_64-linux-gnu.tar.gz \
  && tar -xzf *.tar.gz -C /opt \
  && rm *.tar.gz

COPY docker-entrypoint.sh /entrypoint.sh
COPY bootstrap.sh /bootstrap.sh
COPY start_bitcoind.sh /start_bitcoind.sh

RUN chmod 755 /entrypoint.sh
RUN chmod 755 /bootstrap.sh
RUN chmod 755 /start_bitcoind.sh

EXPOSE 8332 8333 18332 18333 18443 18444 5432 8529

ENTRYPOINT ["/entrypoint.sh"]
CMD ./bootstrap.sh