#!/bin/bash

ps aux | pgrep start_bitcoind | grep -q -v grep
BITCOIND_STATUS=$?
if [ $BITCOIND_STATUS -ne 0 ]; then
  echo "Starting bitcoin daemon..."
  ./start_bitcoind.sh -D
  status=$?
  if [ $status -ne 0 ]; then
    echo "Failed to start bitcoin-etl process: $status"
    exit $status
  fi
fi

while sleep 1; do
    block_count="$(bitcoin-cli -rpcconnect="$BITCOIN_DAEMON_HOST" -rpcuser=user -rpcpassword=password -rpcport=8332 getblockcount)"
    echo "Current block height - $((block_count))"
    sleep 10  
done

echo "Ending bitcoin parser..."
