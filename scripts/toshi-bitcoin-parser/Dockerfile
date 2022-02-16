FROM python:3.10

LABEL maintainer.0="CIBR-QCRI Team"

RUN useradd -r bitcoin \
  && apt-get update -y \
  && apt-get install -y curl gnupg gosu python3-pip postgresql-client\
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* 

ENV BITCOIN_VERSION=0.18.1
ENV BITCOIN_DAEMON_HOST=127.0.0.1
ENV BITCOIN_DAEMON_USERNAME=user
ENV BITCOIN_DAEMON_PASSWORD=password
ENV BITCOIN_DAEMON_PORT=8332
ENV PATH=/opt/bitcoin-${BITCOIN_VERSION}/bin:$PATH

RUN set -ex \
  && curl -SLO https://bitcoin.org/bin/bitcoin-core-${BITCOIN_VERSION}/bitcoin-${BITCOIN_VERSION}-x86_64-linux-gnu.tar.gz \
  && tar -xzf *.tar.gz -C /opt \
  && rm *.tar.gz

RUN mkdir /blockchain-parser
WORKDIR "/blockchain-parser"
COPY bootstrap.sh /bootstrap.sh
COPY process_blockchain.py /process_blockchain.py
COPY btc_blockchain_schema.sql /btc_blockchain_schema.sql

RUN pip3 install bitcoin-etl python-arango

RUN chmod 755 /bootstrap.sh
RUN chmod 755 /process_blockchain.py
RUN chmod 755 /btc_blockchain_schema.sql

EXPOSE 8332 8333 18332 18333 18443 18444 5432 8529

ENTRYPOINT ["/bootstrap.sh"]
