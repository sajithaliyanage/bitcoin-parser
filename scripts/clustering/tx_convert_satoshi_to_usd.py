import time
import csv
import requests
import psycopg2
from psycopg2 import Error
import json

gp_connection = None
gp_cursor = None

date_usd_map = dict()
tx_with_usd = list()
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
    

def convert_satoshi_to_usd(tx_record):
    tx_id = tx_record[0]
    block_time = tx_record[1]
    tx_input_value = tx_record[2]/1e8
    tx_output_value = tx_record[3]/1e8
    block_date = timestamp_to_date(block_time)
    in_usd_amount = btc_to_currency(tx_input_value, block_date)
    out_usd_amount = btc_to_currency(tx_output_value, block_date)
    cur_entry = (tx_id, in_usd_amount, out_usd_amount)

    # print("Covert timestamp:{} to date:{}".format(block_time, block_date))
    # print("Covert satoshi:{} to btc:{} to usd:{}".format(address_record[2], tx_value, usd_amount))
    tx_with_usd.append(cur_entry)


def write_to_csv():
     with open('tx_usd.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(tx_with_usd)


def update_tx_batch():
    gp_cursor.execute( """UPDATE btc_wallet_transaction
                           SET 
                           input_usd_value = update_payload.input_usd_value, 
                           output_usd_value = update_payload.output_usd_value
                           FROM (
                                SELECT (value->>0)::integer AS id, (value->>1)::decimal AS input_usd_value, (value->>2)::decimal AS output_usd_value 
                                FROM json_array_elements(%s)
                            ) update_payload
                           WHERE btc_wallet_transaction.id = update_payload.id""", [json.dumps(tx_with_usd)])
    gp_connection.commit()



def main():
    if not gp_connection or not gp_cursor:
        connects_to_greenplum()

    #load USD amounts into memory
    global date_usd_map
    date_usd_map = get_coindesk_usd_amounts(COINDESK_START, EXCHANGE_END_DATE)
    
    # iterate for total_txes entries
    total_txes = execute_sql_query("SELECT max(id) from btc_wallet_transaction;")
    print("Total txes : ", total_txes[0][0])
    start_index = 0
    end_index = int(total_txes[0][0])
    chunk_size = 1000000
    
    while start_index <= end_index:
        print("Query tx address range {} - {}".format(start_index, start_index+chunk_size))
        tx_with_timestamp = execute_sql_query("select txes.id, block_time, input_value, output_value from (select id, block_number, input_value, output_value from btc_wallet_transaction where id > {} and id <= {}) as txes inner join btc_block on btc_block.height=txes.block_number;".format(start_index, start_index+chunk_size))

        for record in tx_with_timestamp:
            convert_satoshi_to_usd(record)
        print("Calculated usd values for tx range {} - {}".format(start_index, start_index+chunk_size))
        start_index = start_index + chunk_size
        update_tx_batch()
        print("Updated usd values for tx range {} - {}".format(start_index, start_index+chunk_size))
        tx_with_usd.clear()

    # close arangodb connection
    close_gp_connection()


if __name__ == "__main__":
    main()
