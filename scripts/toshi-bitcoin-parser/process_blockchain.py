import json
import time
import csv
import requests
from arango import ArangoClient
from urllib3.connectionpool import xrange
import gc
import pickle
from pathlib import Path
from datetime import date

TYPE_INPUTS_TO = 'inputs_to'
TYPE_OUTPUTS_TO = 'outputs_to'
TYPE_INCLUDED_IN = "included_in"
TYPE_INCLUDES = "includes"
TYPE_PRECEDES = "precedes"
TYPE_SUCCEEDS = "type_succeeds"
TRANSACTION_COLLECTION = 'btc_transactions/{0}'
ADDRESS_COLLECTION = 'btc_addresses/{0}'
BLOCK_COLLECTION = 'btc_blocks/{0}'
MAX_LIST_LIMIT = 100000
COINDESK_START = '2010-07-18'
COINDESK_END_DATE = '2022-03-31'

block_timestamp_map = dict()
usd_exchange_rates = dict()
last_process_date = COINDESK_END_DATE

# Data structures for sql data
block_buffer = list()
transaction_buffer = list()
in_address_buffer = list()
out_address_buffer = list()

# Data structures for non-sql data
block_graph_buffer = list()
tx_buffer = list()
edge_buffer = list()
address_buffer = list()


def update_exchange_rate_map():
    global last_process_date
    global usd_exchange_rates
    load_last_process_date = load_processed_exchange_metadata()
    if load_last_process_date is not None:
        last_process_date = load_last_process_date

    current_date = date.today().strftime("%Y/%m/%d")
    usd_exchange_rates = get_coindesk_usd_amounts(last_process_date, current_date)
    last_process_date = current_date


def exchange_rate(block_date):
    if block_date < COINDESK_START:
        return 0
    
    if block_date > COINDESK_END_DATE:
        update_exchange_rate_map()
    
    return usd_exchange_rates[block_date]


def btc_to_currency(value, date):
    return value * exchange_rate(date)


def timestamp_to_date(epoch):
    return time.strftime('%Y-%m-%d', time.localtime(int(epoch)))


def convert_satoshi_to_usd(satoshi_value, timestamp):
    block_date = timestamp_to_date(timestamp)
    return btc_to_currency(satoshi_value/1e8, block_date)


def get_coindesk_usd_amounts(start_date, end_date):
    base_url = 'https://api.coindesk.com/v1/bpi/historical/close.json'
    r = requests.get('{}?index=USD&currency={}&start={}&end={}'.format(base_url, 'USD', start_date, end_date ))
    r.raise_for_status()
    return r.json()['bpi']


def block_to_graph(block):
    vertex = dict()
    block_number = block['number']
    vertex['_key'] = str(block['number'])
    vertex['hash'] = block['hash']
    vertex['transaction_count'] = block['transaction_count']
    vertex['timestamp'] = block['timestamp']
    block_graph_buffer.append(vertex)

    if block_number > 1:
        edge = dict()
        edge['_from'] = BLOCK_COLLECTION.format(block_number-1)
        edge['_to'] = BLOCK_COLLECTION.format(block_number)
        edge['label'] = TYPE_PRECEDES
        edge_buffer.append(edge)

        edge = dict()
        edge['_from'] = BLOCK_COLLECTION.format(block_number)
        edge['_to'] = BLOCK_COLLECTION.format(block_number - 1)
        edge['label'] = TYPE_SUCCEEDS
        edge_buffer.append(edge)


def tx_to_graph(transaction):
    build_transaction_collection(transaction)
    build_address_collection(transaction)


def build_transaction_collection(transaction):
    vertex = dict()
    vertex['_key'] = transaction['hash']
    vertex['block_number'] = transaction['block_number']
    vertex['index'] = transaction['index']
    vertex['fee'] = transaction['fee']
    vertex['timestamp'] = transaction['block_timestamp']
    vertex['input_value'] = transaction['input_value']
    vertex['output_value'] = transaction['output_value']
    vertex['is_coinbase'] = transaction['is_coinbase']
    tx_buffer.append(vertex)

    edge = dict()
    edge['_from'] = TRANSACTION_COLLECTION.format(transaction['hash'])
    edge['_to'] = BLOCK_COLLECTION.format(transaction['block_number'])
    edge['label'] = TYPE_INCLUDED_IN
    edge_buffer.append(edge)

    edge = dict()
    edge['_from'] = BLOCK_COLLECTION.format(transaction['block_number'])
    edge['_to'] = TRANSACTION_COLLECTION.format(transaction['hash'])
    edge['label'] = TYPE_INCLUDES
    edge_buffer.append(edge)


def build_address_collection(transaction):
    for tx_output in transaction['outputs']:
        for address in tx_output['addresses']:
            create_vertex(address, tx_output['type'])
            create_edge(transaction['hash'], tx_output, address, False)

    for tx_input in transaction['inputs']:
        for address in tx_input['addresses']:
            create_vertex(address, tx_input['type'])
            create_edge(transaction['hash'], tx_input, address, True)


def create_vertex(address, address_type):
    vertex = dict()
    vertex['_key'] = str(address)
    vertex['type'] = address_type
    address_buffer.append(vertex)


def create_edge(tx_hash, tx_io, address, is_input):
    edge = dict()
    edge['required_signatures'] = tx_io['required_signatures']
    if is_input:
        edge['_from'] = ADDRESS_COLLECTION.format(address)
        edge['_to'] = TRANSACTION_COLLECTION.format(tx_hash)
        edge['label'] = TYPE_INPUTS_TO
    else:
        edge['_from'] = TRANSACTION_COLLECTION.format(tx_hash)
        edge['_to'] = ADDRESS_COLLECTION.format(address)
        edge['label'] = TYPE_OUTPUTS_TO
    edge_buffer.append(edge)


def build_transactions(tx):
    transaction = list()
    transaction.append(tx['hash'])
    transaction.append(tx['block_number'])
    transaction.append(tx['index'])
    transaction.append(tx['fee'])
    transaction.append(tx['input_value'])
    transaction.append(tx['output_value'])
    transaction.append(tx['is_coinbase'])
    transaction.append(tx['input_count'])
    transaction.append(tx['output_count'])
    transaction.append(convert_satoshi_to_usd(tx['input_value'], block_timestamp_map[tx['block_number']]))
    transaction.append(convert_satoshi_to_usd(tx['output_value'], block_timestamp_map[tx['block_number']]))
    transaction.append(tx['block_timestamp'])
    transaction_buffer.append(transaction)


def build_in_addresses(tx):
    block_height = tx['block_number']
    for tx_input in tx['inputs']:
        for address in tx_input['addresses']:
            in_address = list()
            in_address.append(tx['hash'])
            in_address.append(address)
            in_address.append(tx_input['type'])
            in_address.append(tx_input['value'])
            in_address.append(convert_satoshi_to_usd(tx_input['value'], block_timestamp_map[block_height]))
            in_address.append(block_height)
            in_address.append(tx['block_timestamp'])
            in_address_buffer.append(in_address)


def build_out_addresses(tx):
    block_height = tx['block_number']
    for tx_output in tx['outputs']:
        for address in tx_output['addresses']:
            out_address = list()
            out_address.append(tx['hash'])
            out_address.append(address)
            out_address.append(tx_output['type'])
            out_address.append(tx_output['value'])
            out_address.append(convert_satoshi_to_usd(tx_output['value'], block_timestamp_map[block_height]))
            out_address.append(block_height)
            out_address.append(tx['block_timestamp'])
            out_address_buffer.append(out_address)


def tx_to_csv(transaction):
    build_transactions(transaction)
    build_in_addresses(transaction)
    build_out_addresses(transaction)

    build_transaction_collection(transaction)
    build_address_collection(transaction)


def block_to_csv(b):
    block = list()
    block.append(b['number'])
    block.append(b['hash'])
    block.append(b['timestamp'])
    block.append(b['transaction_count'])
    block_buffer.append(block)


def write_sql_to_files():
    with open('blocks_sql.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(block_buffer)
    with open('tx_sql.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(transaction_buffer)
    with open('in_addr_sql.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(in_address_buffer)
    with open('out_addr_sql.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(out_address_buffer)
    
    del block_buffer[:]
    del transaction_buffer[:]
    del in_address_buffer[:]
    del out_address_buffer[:]
    gc.collect()


def write_graph_to_files():
    client = ArangoClient(hosts='http://arangodb-cluster:8529')
    sys_db = client.db('_system', username='root', password='')

    if not sys_db.has_database('btc_blockchain'):
        sys_db.create_database('btc_blockchain')

    db = client.db('btc_blockchain', username='root', password='')

    if db.has_graph('btc_blockchain'):
        blockchain = db.graph('btc_blockchain')
    else:
        blockchain = db.create_graph('btc_blockchain')

    bc = db.graph('btc_blockchain')

    if bc.has_vertex_collection("btc_blocks"):
        blocks = bc.vertex_collection("btc_blocks")
    else:
        blocks = bc.create_vertex_collection("btc_blocks")

    if bc.has_vertex_collection("btc_transactions"):
        transactions = bc.vertex_collection("btc_transactions")
    else:
        transactions = bc.create_vertex_collection("btc_transactions")

    if bc.has_vertex_collection("btc_addresses"):
        addresses = bc.vertex_collection("btc_addresses")
    else:
        addresses = bc.create_vertex_collection("btc_addresses")

    if not blockchain.has_edge_definition("btc_edges"):
        edges = blockchain.create_edge_definition(
            edge_collection="btc_edges",
            from_vertex_collections=["btc_blocks", "btc_transactions", "btc_addresses"],
            to_vertex_collections=["btc_blocks", "btc_transactions", "btc_addresses"])
    else:
        edges = blockchain.edge_collection("btc_edges")

    print("[ArangoDB] start adding blocks - ", len(block_graph_buffer))
    try:
        chunks = split_list_as_chunks(block_graph_buffer)
        del block_graph_buffer[:]
        for chunk in chunks:
            blocks.insert_many(chunk)
        chunks.clear()    
    except:
        pass

    print("[ArangoDB] start adding tx - ", len(tx_buffer))
    try:
        chunks = split_list_as_chunks(tx_buffer)
        del tx_buffer[:]
        for chunk in chunks:
            transactions.insert_many(chunk)
        chunks.clear()    
    except:
        pass

    print("[ArangoDB] start adding addresses - ", len(address_buffer))
    try:
        chunks = split_list_as_chunks(address_buffer)
        del address_buffer[:]
        for chunk in chunks:
            addresses.insert_many(chunk)
        chunks.clear()    
    except:
        pass

    print("[ArangoDB] start adding edges - ", len(edge_buffer))
    try:
        chunks = split_list_as_chunks(edge_buffer)
        del edge_buffer[:]
        for chunk in chunks:
            edges.insert_many(chunk)
        chunks.clear()
    except:
        pass

    gc.collect()


def split_list_as_chunks(data_list):
    return [data_list[x:x + MAX_LIST_LIMIT] for x in xrange(0, len(data_list), MAX_LIST_LIMIT)]


def load_data(dict_name):
    wallet_file = Path(dict_name + ".pickle")
    if wallet_file.exists():
        with open(dict_name + '.pickle', 'rb') as f:
            return pickle.load(f)
    else:
        return dict()   


def load_processed_exchange_metadata():
    try:
        if Path("processed_exchange_dates.pickle").exists():
            last_processed_exchange_dates = load_data('processed_exchange_dates')
            exchange_date = last_processed_exchange_dates['last_processed_date']
            if exchange_date is not None:
                return exchange_date
    except:
        pass

    return None


def save_exchange_data():
    with open('usd_exchange_rates.pickle', 'wb') as f:
        pickle.dump(usd_exchange_rates, f, pickle.HIGHEST_PROTOCOL)
    
    del usd_exchange_rates[:]

    if COINDESK_END_DATE != last_process_date:
        last_processed_exchange_dates = dict()
        last_processed_exchange_dates['last_processed_date'] = last_process_date
        with open('processed_exchange_dates.pickle', 'wb') as f:
            pickle.dump(last_processed_exchange_dates, f, pickle.HIGHEST_PROTOCOL)


def main():
    #load USD amounts into memory
    global usd_exchange_rates
    usd_exchange_rates = load_data('usd_exchange_rates')
    if len(usd_exchange_rates) == 0:
        usd_exchange_rates = get_coindesk_usd_amounts(COINDESK_START, COINDESK_END_DATE)

    with open("blocks.json", "r") as f:
        blocks = f.read().replace("\n", ",")[:-1].strip()
        blocks = "[" + blocks + "]"
        blocks = json.loads(blocks)
        for block in blocks:
            block_timestamp_map[block['number']] = block['timestamp']
            block_to_csv(block)
            block_to_graph(block)
        # sort blocks by block_height
        block_buffer.sort(key=lambda x: x[0])

    with open("enriched_transactions.json", "r") as f:
        transactions = f.read().replace("\n", ",")[:-1].strip()
        transactions = "[" + transactions + "]"
        transactions = json.loads(transactions)
        for tx in transactions:
            tx_to_csv(tx)
            tx_to_graph(tx)
        block_timestamp_map.clear()

    write_sql_to_files()
    write_graph_to_files()
    save_exchange_data()


if __name__ == "__main__":
    main()