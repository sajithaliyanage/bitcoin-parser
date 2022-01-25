import json
import csv
from arango import ArangoClient
from urllib3.connectionpool import xrange

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
    # transaction.append(tx['block_timestamp'])
    transaction.append(tx['input_value'])
    transaction.append(tx['output_value'])
    transaction.append(tx['is_coinbase'])
    transaction.append(tx['input_count'])
    transaction.append(tx['output_count'])
    transaction_buffer.append(transaction)


def build_in_addresses(tx):
    for tx_input in tx['inputs']:
        for address in tx_input['addresses']:
            in_address = list()
            in_address.append(tx['hash'])
            in_address.append(address)
            in_address.append(tx_input['type'])
            in_address.append(tx_input['value'])
            in_address_buffer.append(in_address)


def build_out_addresses(tx):
    for tx_output in tx['outputs']:
        for address in tx_output['addresses']:
            out_address = list()
            out_address.append(tx['hash'])
            out_address.append(address)
            out_address.append(tx_output['type'])
            out_address.append(tx_output['value'])
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


def write_graph_to_files():
    client = ArangoClient(hosts='http://arangodb-cluster-int:8529')
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
        edges = blockchain.edge_collection("edges")

    print("[ArangoDB] start adding blocks - ", len(block_graph_buffer))
    try:
        chunks = split_list_as_chunks(block_graph_buffer)
        for chunk in chunks:
            blocks.insert_many(chunk)
    except:
        pass

    print("[ArangoDB] start adding tx - ", len(tx_buffer))
    try:
        chunks = split_list_as_chunks(tx_buffer)
        for chunk in chunks:
            transactions.insert_many(chunk)
    except:
        pass

    print("[ArangoDB] start adding addresses - ", len(address_buffer))
    try:
        chunks = split_list_as_chunks(address_buffer)
        for chunk in chunks:
            addresses.insert_many(chunk)
    except:
        pass

    print("[ArangoDB] start adding edges - ", len(edge_buffer))
    try:
        chunks = split_list_as_chunks(edge_buffer)
        for chunk in chunks:
            edges.insert_many(chunk)
    except:
        pass


    block_graph_buffer.clear()
    tx_buffer.clear()
    edge_buffer.clear()
    address_buffer.clear()


def split_list_as_chunks(data_list):
    return [data_list[x:x + MAX_LIST_LIMIT] for x in xrange(0, len(data_list), MAX_LIST_LIMIT)]


def main():
    with open("blocks.json", "r") as f:
        blocks = f.read().replace("\n", ",")[:-1].strip()
        blocks = "[" + blocks + "]"
        blocks = json.loads(blocks)
        for block in blocks:
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

    write_sql_to_files()
    write_graph_to_files()


if __name__ == "__main__":
    main()