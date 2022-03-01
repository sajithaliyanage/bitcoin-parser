from arango import ArangoClient
from urllib3.connectionpool import xrange
import gc
import psycopg2
from psycopg2 import Error

WALLET_COLLECTION = 'btc_wallets/{0}'
MAX_LIST_LIMIT = 100000
gp_connection = None
gp_cursor = None
arango_connection = None
MAX_CURSOR_LIMIT = 5000000

wallet_buffer = list()
edge_buffer = list()


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


def connects_to_arango():
    try:
        # Connect to an existing database
        global arango_connection
        client = ArangoClient(hosts='http://10.4.8.131:8529')
        arango_connection = client.db('btc_blockchain', username='root', password='')

        print("You are connected to - ArangoDB\n")
        return

    except (Exception, Error) as error:
        print("Error while connecting to ArangoDB", error)


def execute_sql_query(query):
    gp_cursor.execute(query)
    return gp_cursor.fetchall()

def execute_sql_batch_query(query):
    gp_cursor.execute(query)
    return gp_cursor


def close_gp_connection():
    try:
        if (gp_connection):
            gp_cursor.close()
            gp_connection.close()
            print("PostgreSQL connection is closed")
    except (Exception, Error) as error:
        print("Error while closing the connection to PostgreSQL", error)


def close_arango_connection():
    try:
        if (arango_connection):
            arango_connection.close()
            print("ArangoDB connection is closed")
    except (Exception, Error) as error:
        print("Error while closing the connection to ArangoDB", error)


def create_edge(wallet_in, wallet_out, tx_hash, in_amount, out_amount):
    edge = dict()
    edge['_from'] = WALLET_COLLECTION.format(wallet_in)
    edge['_to'] = WALLET_COLLECTION.format(wallet_out)
    edge['tx_hash'] = tx_hash
    edge['in_amount'] = in_amount
    edge['out_amount'] = out_amount
    edge_buffer.append(edge)


def write_wallet_vertex():
    if arango_connection.has_graph('btc_wallet_cluster'):
        arango_connection.graph('btc_wallet_cluster')
    else:
        arango_connection.create_graph('btc_wallet_cluster')

    wc = arango_connection.graph('btc_wallet_cluster')
    if wc.has_vertex_collection("btc_wallets"):
        wallets = wc.vertex_collection("btc_wallets")
    else:
        wallets = wc.create_vertex_collection("btc_wallets")

    print("[ArangoDB] start adding wallets - ", len(wallet_buffer))
    try:
        chunks = split_list_as_chunks(wallet_buffer)
        print("Available chunks to be processed - ", len(chunks))
        wallet_buffer.clear()
        count = 0
        for chunk in chunks:
            wallets.insert_many(chunk)
            count = count + len(chunk)
            print("Processed another 100000 wallets - vertices, total: ", count)
                
        chunks.clear()    
    except:
        pass

    gc.collect()


def write_wallet_edges():
    if arango_connection.has_graph('btc_wallet_cluster'):
        arango_connection.graph('btc_wallet_cluster')
    else:
        arango_connection.create_graph('btc_wallet_cluster')

    wc = arango_connection.graph('btc_wallet_cluster')

    if not wc.has_edge_definition("btc_wallet_wallet_edges"):
        edges = wc.create_edge_definition(
            edge_collection="btc_wallet_wallet_edges",
            from_vertex_collections=["btc_wallets", "btc_addresses"],
            to_vertex_collections=["btc_wallets", "btc_addresses"])
    else:
        edges = wc.edge_collection("btc_wallet_wallet_edges")


    print("[ArangoDB] start adding wallet edges - ", len(edge_buffer))
    try:
        chunks = split_list_as_chunks(edge_buffer)
        print("Chunk count to be processed - ", len(chunks))
        del edge_buffer[:]
        for chunk in chunks:
            edges.insert_many(chunk)
        chunks.clear()    
    except:
        pass

    gc.collect()


def split_list_as_chunks(data_list):
    return [data_list[x:x + MAX_LIST_LIMIT] for x in xrange(0, len(data_list), MAX_LIST_LIMIT)]


def wallet_to_graph(wallet_id):
    vertex = dict()
    vertex['_key'] = wallet_id
    wallet_buffer.append(vertex)


def main():
    if not gp_connection or not gp_cursor:
        connects_to_greenplum()

    if not arango_connection:
        connects_to_arango()
    
    # insert wallet vertices
    btc_wallet_records = execute_sql_query("SELECT cluster_id from btx_wallet;")
    print("Total wallet count - {}".format(len(btc_wallet_records)))
    for input_row in btc_wallet_records:
        wallet_id = input_row[0]
        wallet_to_graph(wallet_id)
    write_wallet_vertex()
    
    total_addresses = execute_sql_query("SELECT max(id) from btc_address_cluster;")
    print("Total addresses: ", total_addresses[0][0])
    start_index = 52849782
    end_index = int(total_addresses[0][0])
    chunk_size = 1000000
    
    while start_index <= end_index:
        cursor = gp_connection.cursor(name='fetch_large_result')
        print("Query wallet map for address range {} - {}".format(start_index, start_index+chunk_size))
        # get all wallet->wallet map by tx_hash of this address range
        cursor.execute("select wallet_in, wallet_out, in_wallets.tx_hash as tx_hash, input_amount, output_amount from (SELECT btc_address_cluster.address as address, tx_hash, tx_value as input_amount, cluster_id as wallet_in from btc_address_cluster INNER JOIN btc_tx_input on btc_address_cluster.address=btc_tx_input.address where btc_address_cluster.id > {} and btc_address_cluster.id <= {} order by btc_address_cluster.id asc) as in_wallets inner join (SELECT btc_address_cluster.address as address, tx_hash, tx_value as output_amount, cluster_id as wallet_out from btc_address_cluster INNER JOIN btc_tx_output on btc_address_cluster.address=btc_tx_output.address where btc_address_cluster.id > {} and btc_address_cluster.id <= {} order by btc_address_cluster.id asc) as out_wallets on in_wallets.tx_hash=out_wallets.tx_hash;".format(start_index, start_index+chunk_size, start_index, start_index+chunk_size))
        fetched_total_count = 0
        print("Query executed for address range {} - {}".format(start_index, start_index+chunk_size))
        while True:
            records = cursor.fetchmany(size=MAX_CURSOR_LIMIT)
            if not records:
                break
            fetched_total_count = fetched_total_count + len(records)
            print("Fetched wallet-wallet edges count - {}".format(fetched_total_count))
            for address in records:
                create_edge(address[0], address[1], address[2], address[3], address[4])
            write_wallet_edges()
        cursor.close()
        start_index = start_index + chunk_size

    # close arangodb connection
    close_gp_connection()


if __name__ == "__main__":
    main()
