#!/bin/bash

PROVIDER_URI="http://user:password@localhost:8332"
export LC_ALL=C.UTF-8
export LANG=C.UTF-8
purge_data() {
  rm transactions.json blocks.json enriched_transactions.json
}

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

#psql -h "$GREENPLUM_SERVICE_HOST" -p "$GREENPLUM_SERVICE_PORT" --user=gpadmin -f blockstack_schema.sql

export last_block=0
while sleep 1; do
    block_count="$(bitcoin-cli -rpcuser=user -rpcpassword=password -rpcport=8332 getblockcount)"
    if (( block_count > ((last_block+105)) )); then
        echo "Processing start for block range $((last_block+1))-$((last_block+100))"
        bitcoinetl export_blocks_and_transactions --start-block "$((last_block+1))" --end-block "$((last_block+100))" \
        --provider-uri $PROVIDER_URI --chain bitcoin --blocks-output blocks.json --transactions-output transactions.json && \
        bitcoinetl enrich_transactions --provider-uri $PROVIDER_URI --transactions-input transactions.json \
        --transactions-output enriched_transactions.json  && \
        python3 chain_formatter.py && \
        #psql -h "$GREENPLUM_SERVICE_HOST" -p "$GREENPLUM_SERVICE_PORT" -d blockstack --user=gpadmin -c "\\COPY btc_block(height, hash, block_time, tx_count) FROM blocks_sql.csv CSV DELIMITER E','"
        #psql -h "$GREENPLUM_SERVICE_HOST" -p "$GREENPLUM_SERVICE_PORT" -d blockstack --user=gpadmin -c "\\COPY btc_transaction(hash, block_number, index, fee, input_value, output_value, is_coinbase, input_count, output_count) FROM tx_sql.csv CSV DELIMITER E','"
        #psql -h "$GREENPLUM_SERVICE_HOST" -p "$GREENPLUM_SERVICE_PORT" -d blockstack --user=gpadmin -c "\\COPY btc_input_address(tx_hash, address, address_type, tx_value) FROM in_addr_sql.csv CSV DELIMITER E','"
        #psql -h "$GREENPLUM_SERVICE_HOST" -p "$GREENPLUM_SERVICE_PORT" -d blockstack --user=gpadmin -c "\\COPY btc_output_address(tx_hash, address, address_type, tx_value) FROM out_addr_sql.csv CSV DELIMITER E','"
        purge_data
        export last_block=$((last_block+100))
    fi
done

echo "Ending bitcoin parser..."
