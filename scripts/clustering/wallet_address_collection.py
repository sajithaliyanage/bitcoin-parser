from arango import ArangoClient
from urllib3.connectionpool import xrange
import gc
import psycopg2
from psycopg2 import Error

WALLET_COLLECTION = 'btc_wallets/{0}'
ADDRESS_COLLECTION = 'btc_addresses/{0}'
MAX_LIST_LIMIT = 100000
gp_connection = None
gp_cursor = None
arango_connection = None

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


def create_edge(wallet, address):
    edge = dict()
    edge['_from'] = WALLET_COLLECTION.format(wallet)
    edge['_to'] = ADDRESS_COLLECTION.format(address)
    edge_buffer.append(edge)


def write_wallet_address_edges():
    if arango_connection.has_graph('btc_wallet_cluster'):
        arango_connection.graph('btc_wallet_cluster')
    else:
        arango_connection.create_graph('btc_wallet_cluster')

    wc = arango_connection.graph('btc_wallet_cluster')

    if not wc.has_edge_definition("btc_wallet_address_edges"):
        edges = wc.create_edge_definition(
            edge_collection="btc_wallet_address_edges",
            from_vertex_collections=["btc_wallets", "btc_addresses"],
            to_vertex_collections=["btc_wallets", "btc_addresses"])
    else:
        edges = wc.edge_collection("btc_wallet_address_edges")


    print("[ArangoDB] start adding wallet edges - ", len(edge_buffer))
    try:
        chunks = split_list_as_chunks(edge_buffer)
        print("Chunk count to be processed - ", len(chunks))
        edge_buffer.clear()
        for chunk in chunks:
            edges.insert_many(chunk)
        chunks.clear()
    except:
        pass

    gc.collect()


def split_list_as_chunks(data_list):
    return [data_list[x:x + MAX_LIST_LIMIT] for x in xrange(0, len(data_list), MAX_LIST_LIMIT)]


def main():
    if not gp_connection or not gp_cursor:
        connects_to_greenplum()

    if not arango_connection:
        connects_to_arango()
    
    total_addresses = execute_sql_query("SELECT max(id) from btc_address_cluster;")
    print("Total addresses: ", total_addresses[0][0])
    start_index = 0
    end_index = int(total_addresses[0][0])
    chunk_size = 5000000
    
    while start_index <= end_index:
        print("Query wallet map for address range {} - {}".format(start_index, start_index+chunk_size))
        records = execute_sql_query("SELECT cluster_id, address from btc_address_cluster where id > {} and id <= {};".format(start_index, start_index+chunk_size))
        for address in records:
            create_edge(address[0], address[1])

        write_wallet_address_edges()
        start_index = start_index + chunk_size

    # close arangodb connection
    close_gp_connection()


if __name__ == "__main__":
    main()
