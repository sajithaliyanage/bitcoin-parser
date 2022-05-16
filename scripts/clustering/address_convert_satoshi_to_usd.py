import time
import csv
import requests
import psycopg2
from psycopg2 import Error
import json

gp_connection = None
gp_cursor = None

date_usd_map = dict()
inputs_with_usd = list()
outputs_with_usd = list()
COINDESK_START = '2010-07-18'
EXCHANGE_END_DATE = '2015-03-31'

def connects_to_greenplum():
    try:
        # Connect to an existing database
        global gp_connection
        gp_connection = psycopg2.connect(user="gpadmin",
                                    password="",
                                    host="10.4.8.131",
                                    port="5432",
                                    database="btc_blockchain")

        # Create a cursor to perform database operations
        global gp_cursor
        gp_cursor = gp_connection.cursor()
        gp_cursor.execute("SELECT version();")
        record = gp_cursor.fetchone()
        print("You are connected to - ", record, "\n")
        return

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)


def execute_sql_query(query):
    gp_cursor.execute(query)
    return gp_cursor.fetchall()


def close_gp_connection():
    try:
        if (gp_connection):
            gp_cursor.close()
            gp_connection.close()
            print("PostgreSQL connection is closed")
    except (Exception, Error) as error:
        print("Error while closing the connection to PostgreSQL", error)


def get_coindesk_usd_amounts(start_date, end_date):
    base_url = 'https://api.coindesk.com/v1/bpi/historical/close.json'
    r = requests.get('{}?index=USD&currency={}&start={}&end={}'.format(base_url, 'USD', start_date, end_date ))
    r.raise_for_status()
    return r.json()['bpi']


def timestamp_to_date(epoch):
    return time.strftime('%Y-%m-%d', time.localtime(int(epoch)))


def exchangerate(block_date):
    if block_date < COINDESK_START:
        return 0
    return date_usd_map[block_date]


def btc_to_currency(value, date):
    return value * exchangerate(date)
    

def convert_satoshi_to_usd(address_record, store_list):
    address_id = address_record[0]
    block_time = address_record[1]
    tx_value = address_record[2]/1e8
    block_date = timestamp_to_date(block_time)
    usd_amount = btc_to_currency(tx_value, block_date)
    cur_entry = (address_id, usd_amount)

    # print("Covert timestamp:{} to date:{}".format(block_time, block_date))
    # print("Covert satoshi:{} to btc:{} to usd:{}".format(address_record[2], tx_value, usd_amount))
    store_list.append(cur_entry)


def write_to_csv(is_input):
    if is_input:
        with open('input_usd.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(inputs_with_usd)
    else:
        with open('output_usd.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(outputs_with_usd)


def update_input_batch():
    gp_cursor.execute( """UPDATE btc_tx_input 
                           SET usd_value = update_payload.usd 
                           FROM (
                                SELECT (value->>0)::integer AS id, (value->>1)::decimal AS usd 
                                FROM json_array_elements(%s)
                            ) update_payload
                           WHERE btc_tx_input.id = update_payload.id""", [json.dumps(inputs_with_usd)])
    gp_connection.commit()


def update_output_batch():
    gp_cursor.execute( """UPDATE btc_tx_output 
                           SET usd_value = update_payload.usd 
                           FROM (
                                SELECT (value->>0)::integer AS id, (value->>1)::decimal AS usd 
                                FROM json_array_elements(%s)
                           ) update_payload 
                           WHERE btc_tx_output.id = update_payload.id""", [json.dumps(outputs_with_usd)])
    gp_connection.commit()


def main():
    if not gp_connection or not gp_cursor:
        connects_to_greenplum()

    #load USD amounts into memory
    global date_usd_map
    date_usd_map = get_coindesk_usd_amounts(COINDESK_START, EXCHANGE_END_DATE)
    
    # iterate for input entries
    total_inputs = execute_sql_query("SELECT max(id) from btc_tx_input;")
    print("Total input addresses: ", total_inputs[0][0])
    start_index = 0
    end_index = int(total_inputs[0][0])
    chunk_size = 1000000
    
    while start_index <= end_index:
        print("Query input address range {} - {}".format(start_index, start_index+chunk_size))
        input_with_timestamp = execute_sql_query("select input_id, block_time, tx_value from (select inputs.id as input_id, inputs.tx_hash as tx_hash, block_number, tx_value from (select id, tx_hash, tx_value from btc_tx_input where id > {} and id <= {}) as inputs join btc_transaction on btc_transaction.hash=inputs.tx_hash) as txes join btc_block on btc_block.height=txes.block_number order by input_id asc;".format(start_index, start_index+chunk_size))

        for record in input_with_timestamp:
            convert_satoshi_to_usd(record, inputs_with_usd)
       
        start_index = start_index + chunk_size
        update_input_batch()
        print("Updated usd values input address range {} - {}".format(start_index, start_index+chunk_size))
        inputs_with_usd.clear()
    # write_to_csv(True)


    # iterate for output entries
    total_outputs = execute_sql_query("SELECT max(id) from btc_tx_output;")
    print("Total output addresses: ", total_outputs[0][0])
    start_index = 0
    end_index = int(total_outputs[0][0])
    chunk_size = 1000000
    
    while start_index <= end_index:
        print("Query output address range {} - {}".format(start_index, start_index+chunk_size))
        output_with_timestamp = execute_sql_query("select output_id, block_time, tx_value from (select outputs.id as output_id, outputs.tx_hash as tx_hash, block_number, tx_value from (select id, tx_hash, tx_value from btc_tx_output where id > {} and id <= {}) as outputs join btc_transaction on btc_transaction.hash=outputs.tx_hash) as txes join btc_block on btc_block.height=txes.block_number order by output_id asc;".format(start_index, start_index+chunk_size))

        for record in output_with_timestamp:
            convert_satoshi_to_usd(record, outputs_with_usd)
       
        start_index = start_index + chunk_size
        update_output_batch()
        print("Updated usd values output address range {} - {}".format(start_index, start_index+chunk_size))
        outputs_with_usd.clear()
    write_to_csv(False)

    # close arangodb connection
    close_gp_connection()


if __name__ == "__main__":
    main()
