#!/bin/bash

PROVIDER_URI="http://user:password@localhost:8332"
export LC_ALL=C.UTF-8
export LANG=C.UTF-8
purge_data() {
  rm transactions.json blocks.json enriched_transactions.json
  rm blocks_sql.csv tx_sql.csv in_addr_sql.csv out_addr_sql.csv
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

STORED_BLOCK_HEIGHT=$(psql -h "$GREENPLUM_SERVICE_HOST" -p "$GREENPLUM_SERVICE_PORT" -d btc_blockchain --user=gpadmin -c "SELECT height FROM btc_block ORDER BY id DESC LIMIT 1;" | sed -n '3p' | xargs)
re='^[0-9]+$'
if [[ $STORED_BLOCK_HEIGHT =~ $re ]] ; then
   START_BLOCK_HEIGHT=$STORED_BLOCK_HEIGHT
fi
echo "Staring block height is $((START_BLOCK_HEIGHT))"

psql -h "$GREENPLUM_SERVICE_HOST" -p "$GREENPLUM_SERVICE_PORT" --user=gpadmin -f blockstack_schema.sql

export start_block_height=${START_BLOCK_HEIGHT}
export end_block_height=${END_BLOCK_HEIGHT}
export parse_chunk=${BATCH_SIZE}
while sleep 1; do
    block_count="$(bitcoin-cli -rpcuser=user -rpcpassword=password -rpcport=8332 getblockcount)"
    echo "Current block height - $((block_count))"
    if ((block_count > start_block_height+parse_chunk+5)) && ((end_block_height >= start_block_height+parse_chunk)); then
        echo "Processing for block range $((start_block_height+1))-$((start_block_height+parse_chunk))"
        bitcoinetl export_blocks_and_transactions --start-block "$((start_block_height+1))" --end-block "$((start_block_height+parse_chunk))" \
        --provider-uri $PROVIDER_URI --batch-size 10 --max-workers 10 --chain bitcoin --blocks-output blocks.json --transactions-output transactions.json && \
        bitcoinetl enrich_transactions --provider-uri $PROVIDER_URI --batch-size 10 --max-workers 10 --transactions-input transactions.json \
        --transactions-output enriched_transactions.json  && \
        echo "Blocks exported from bitcoin-etl range $((start_block_height+1))-$((start_block_height+parse_chunk))"
        python3 process_blockchain.py && \
        psql -h "$GREENPLUM_SERVICE_HOST" -p "$GREENPLUM_SERVICE_PORT" -d btc_blockchain --user=gpadmin -c "\\COPY btc_block(height, hash, block_time, tx_count) FROM blocks_sql.csv CSV DELIMITER E','"
        psql -h "$GREENPLUM_SERVICE_HOST" -p "$GREENPLUM_SERVICE_PORT" -d btc_blockchain --user=gpadmin -c "\\COPY btc_transaction(hash, block_number, index, fee, input_value, output_value, is_coinbase, input_count, output_count) FROM tx_sql.csv CSV DELIMITER E','"
        psql -h "$GREENPLUM_SERVICE_HOST" -p "$GREENPLUM_SERVICE_PORT" -d btc_blockchain --user=gpadmin -c "\\COPY btc_tx_input(tx_hash, address, address_type, tx_value) FROM in_addr_sql.csv CSV DELIMITER E','"
        psql -h "$GREENPLUM_SERVICE_HOST" -p "$GREENPLUM_SERVICE_PORT" -d btc_blockchain --user=gpadmin -c "\\COPY btc_tx_output(tx_hash, address, address_type, tx_value) FROM out_addr_sql.csv CSV DELIMITER E','"
        echo "Data successfully uploaded to GreenplumpDB from block range $((start_block_height+1))-$((start_block_height+parse_chunk))"
        purge_data
        export start_block_height=$((start_block_height+parse_chunk))
    elif ((block_count > start_block_height+parse_chunk+5)) && ((end_block_height < start_block_height+parse_chunk)) && ((end_block_height > start_block_height)); then
        echo "Processing for block range $((start_block_height+1))-$((end_block_height))"
        bitcoinetl export_blocks_and_transactions --start-block "$((start_block_height+1))" --end-block "$((end_block_height))" \
        --provider-uri $PROVIDER_URI --batch-size 10 --max-workers 10 --chain bitcoin --blocks-output blocks.json --transactions-output transactions.json && \
        bitcoinetl enrich_transactions --provider-uri $PROVIDER_URI --batch-size 10 --max-workers 10 --transactions-input transactions.json \
        --transactions-output enriched_transactions.json  && \
        echo "Blocks exported from bitcoin-etl range $((start_block_height+1))-$((end_block_height))"
        python3 process_blockchain.py && \
        psql -h "$GREENPLUM_SERVICE_HOST" -p "$GREENPLUM_SERVICE_PORT" -d btc_blockchain --user=gpadmin -c "\\COPY btc_block(height, hash, block_time, tx_count) FROM blocks_sql.csv CSV DELIMITER E','"
        psql -h "$GREENPLUM_SERVICE_HOST" -p "$GREENPLUM_SERVICE_PORT" -d btc_blockchain --user=gpadmin -c "\\COPY btc_transaction(hash, block_number, index, fee, input_value, output_value, is_coinbase, input_count, output_count) FROM tx_sql.csv CSV DELIMITER E','"
        psql -h "$GREENPLUM_SERVICE_HOST" -p "$GREENPLUM_SERVICE_PORT" -d btc_blockchain --user=gpadmin -c "\\COPY btc_tx_input(tx_hash, address, address_type, tx_value) FROM in_addr_sql.csv CSV DELIMITER E','"
        psql -h "$GREENPLUM_SERVICE_HOST" -p "$GREENPLUM_SERVICE_PORT" -d btc_blockchain --user=gpadmin -c "\\COPY btc_tx_output(tx_hash, address, address_type, tx_value) FROM out_addr_sql.csv CSV DELIMITER E','"
        echo "Data successfully uploaded to GreenplumpDB from block range $((start_block_height+1))-$((end_block_height))"
        purge_data
        export start_block_height=$((end_block_height))
    elif ((end_block_height == start_block_height)); then
        echo "Parsing completed for the given upper bound block height - $((end_block_height))"
    fi
done

echo "Ending bitcoin parser..."
