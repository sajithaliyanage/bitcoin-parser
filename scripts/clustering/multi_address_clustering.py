import psycopg2
from psycopg2 import Error
import uuid
import pickle
from pathlib import Path
import sys
import csv

gp_connection = None
gp_cursor = None
last_processed_input_id = 0
processing_row_count = 10000000
last_processed_tx_hash = None
last_processed_tx_wallet_id = None

# Data structures for heuristic-1 clustering
address_wallet_map = dict()
wallet_to_wallet_map = dict()
wallet_final_state_map = dict()
wallet_temp_map = dict()

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


def insert_sql_query(query):
    gp_cursor.execute(query)
    gp_connection.commit()
    print("Record inserted successfully ")


def update_sql_query(query):
    gp_cursor.execute(query)
    gp_connection.commit()
    count = gp_cursor.rowcount
    print(count, "Record updated successfully ")


def delete_sql_query(query):
    gp_cursor.execute(query)
    gp_connection.commit()
    count = gp_cursor.rowcount
    print(count, "Record deleted successfully ")


def close_gp_connection():
    try:
        if (gp_connection):
            gp_cursor.close()
            gp_connection.close()
            print("PostgreSQL connection is closed")
    except (Exception, Error) as error:
        print("Error while closing the connection to PostgreSQL", error)


def multi_address__clustering_heuristic():

    global last_processed_input_id
    global last_processed_tx_hash
    global last_processed_tx_wallet_id

    # Gets all the input addresses stored
    query = "SELECT id, tx_hash, address, tx_value from btc_tx_input where id > {} and id <= {} order by id asc;".format(last_processed_input_id, int(last_processed_input_id + processing_row_count))
    print(query)
    tx_inputs = execute_sql_query(query)
    #tx_inputs = [ [ 1, 'XX', 'A', 'C'], [ 2 ,  'XX', 'D', 'F'], [ 3,  'HH','A', 'K'], [ 4,  'DD', 'D'], [5,  'ZZ', 'P'], [6,  'ZZ', 'D'],  [7, 'PP', 'P' ] ]
    #tx_limit = last_processed_input_id + 7
    #tx_inputs = tx_inputs[last_processed_input_id:tx_limit:1]
    print(len(tx_inputs), " - Input addresses loaded from the databse")

    generated_wallet_id = last_processed_tx_wallet_id
    count = 0
    for input_row in tx_inputs:
        id = input_row[0]
        tx_hash = input_row[1]
        input_address = input_row[2]

        # print("current address - ", input_address)
        # print("previous process tx_hash ", last_processed_tx_hash)
        # print("current process tx_hash ", tx_hash)
        # generate new UUID for new TX inputs
        if tx_hash != last_processed_tx_hash or generated_wallet_id is None:
            # print("generating wallet id .....")
            generated_wallet_id = str(uuid.uuid4())
            last_processed_tx_hash = tx_hash
            last_processed_tx_wallet_id = generated_wallet_id

        # print("current generated_wallet_id - ", generated_wallet_id)
        wallet_id = address_wallet_map.get(input_address)
        if wallet_id is None:
            # print("Not found in address_wallet_map")
            address_wallet_map[input_address] = generated_wallet_id
            # print("Add to the address_wallet_map since wallet_id not in map")
        else:
            # print("Found in address_wallet_map")
            start_wallet_id = wallet_id
            temp_final_wallet_id = wallet_temp_map.get(start_wallet_id)
            if temp_final_wallet_id is not None:
                wallet_id = temp_final_wallet_id
            
            while True:
                # print("current wallet_id - " ,wallet_id)
                traverse_wallet_id = wallet_to_wallet_map.get(wallet_id)
                # print("current traverse_wallet_id - ", traverse_wallet_id)
                if traverse_wallet_id is None:
                    if wallet_id != generated_wallet_id:
                        wallet_to_wallet_map[wallet_id] = generated_wallet_id
                        # print("Rule added to wallet_to_wallet_map " + wallet_id + " -> " + generated_wallet_id)
                    
                    if start_wallet_id != generated_wallet_id:
                        wallet_temp_map[start_wallet_id] = generated_wallet_id
                    break
                else:
                    # print("new value for wallet_id - ", traverse_wallet_id)
                    wallet_id = traverse_wallet_id
                        
        
        # print("\n ------------------------- \n")
        last_processed_input_id = id
        count = count + 1

        if count % 100000 == 0:
            print("Processed another 100000 input address, total: ", count)


def save_wallet_data():
    with open('address_wallet_map.pickle', 'wb') as f:
        pickle.dump(address_wallet_map, f, pickle.HIGHEST_PROTOCOL)

    with open('wallet_to_wallet_map.pickle', 'wb') as f:
        pickle.dump(wallet_to_wallet_map, f, pickle.HIGHEST_PROTOCOL)

    with open('wallet_temp_map.pickle', 'wb') as f:
        pickle.dump(wallet_temp_map, f, pickle.HIGHEST_PROTOCOL) 
    
    last_processed_input_data_map = dict()
    last_processed_input_data_map['last_id'] = last_processed_input_id
    last_processed_input_data_map['last_tx_hash'] = last_processed_tx_hash
    last_processed_input_data_map['last_tx_wallet_id'] = last_processed_tx_wallet_id
    with open('last_processed_input_data.pickle', 'wb') as f:
        pickle.dump(last_processed_input_data_map, f, pickle.HIGHEST_PROTOCOL) 


def load_wallet_data(dict_name):
    wallet_file = Path(dict_name + ".pickle")
    if wallet_file.exists():
        with open(dict_name + '.pickle', 'rb') as f:
            return pickle.load(f)
    else:
        return dict()            


def load_last_processed_input_metadata():
    try:
        if Path("last_processed_input_data.pickle").exists():
            last_processed_input_data_map = load_wallet_data('last_processed_input_data')
            last_processed_input_id = last_processed_input_data_map['last_id']
            last_processed_tx_hash = last_processed_input_data_map['last_tx_hash']
            last_processed_tx_wallet_id = last_processed_input_data_map['last_tx_wallet_id']
            if last_processed_input_data_map is not None and last_processed_input_id is not None and last_processed_tx_hash is not None and last_processed_tx_wallet_id is not None:
                return int(last_processed_input_id), last_processed_tx_hash, last_processed_tx_wallet_id
    except:
        pass

    return 0, None, None


def post_process_wallet_data():
    count = 0
    for key,value in address_wallet_map.items():
        wallet_id = value
        while True:
            # check wallet_id is already traversed and detect the final wallet_id
            cached_wallet_final_id = wallet_final_state_map.get(wallet_id)
            if cached_wallet_final_id is not None:
                wallet_id = cached_wallet_final_id

            temp_final_wallet_id = wallet_temp_map.get(wallet_id)
            if temp_final_wallet_id is not None:
                wallet_id = temp_final_wallet_id

            # check final traversal wallet_id for the given wallet_id
            traverse_wallet_id = wallet_to_wallet_map.get(wallet_id)
            if traverse_wallet_id is None:
                wallet_final_state_map[value] = wallet_id
                break
            else:
                wallet_id = traverse_wallet_id
        
        # update new wallet_id
        address_wallet_map[key] = wallet_id
        count = count + 1

        if count % 100000 == 0:
            print("Processed another 100000 input address, total: ", count)

    print("Multi address clustering - wallet mapping completed successfully")

    # save data in a csv file
    file_name = "address_wallet_mapping.csv"
    with open(file_name, 'w') as csv_file:  
        writer = csv.writer(csv_file)
        for key, value in address_wallet_map.items():
            writer.writerow([key, value])

    print("Multi address clustering - wallet mapping successfully write into a file: ", file_name)


def print_wallet_data_structure():
    for key,value in address_wallet_map.items():
        print(key ," -> ", value)

    print('\n')
    for key,value in wallet_to_wallet_map.items():
        print(key ," -> ", value)


def clear_data():
    address_wallet_map.clear()
    wallet_to_wallet_map.clear()
    wallet_final_state_map.clear()
    wallet_temp_map.clear()


def main():
    
    # read previous run wallet metadata
    global last_processed_input_id
    global last_processed_tx_hash
    global last_processed_tx_wallet_id
    last_processed_input_id,last_processed_tx_hash,last_processed_tx_wallet_id = load_last_processed_input_metadata()

    # load wallet_to_wallet_map and wallet_address_map to the memory if exists
    if last_processed_input_id != 0:
        global address_wallet_map 
        address_wallet_map = load_wallet_data('address_wallet_map')
        print('Loaded {0} address_wallet_map entries to the memory'.format(len(address_wallet_map)))

        global wallet_to_wallet_map
        wallet_to_wallet_map = load_wallet_data('wallet_to_wallet_map')
        print('Loaded {0} wallet_to_wallet_map entries to the memory'.format(len(wallet_to_wallet_map)))

        global wallet_temp_map
        wallet_temp_map = load_wallet_data('wallet_temp_map')
        print('Loaded {0} wallet_temp_map entries to the memory'.format(len(wallet_temp_map)))


    if len(sys.argv) >= 2 and sys.argv[1] == 'wallet_parser':
        global wallet_final_state_map 
        wallet_final_state_map = load_wallet_data('wallet_final_state_map')
        print('Loaded {0} wallet_final_state_map entries to the memory'.format(len(wallet_final_state_map)))

        # process wallet mapping and identify final wallet_id for the addresses
        print("Script started to create cluster from loaded input address id range: {} to {}".format(last_processed_input_id, last_processed_input_id + processing_row_count))
        post_process_wallet_data()

        with open('wallet_final_state_map.pickle', 'wb') as f:
            pickle.dump(wallet_final_state_map, f, pickle.HIGHEST_PROTOCOL)
    else:
        if not gp_connection or not gp_cursor:
            connects_to_greenplum()
        # process clustering
        print("Script started to process input address id range: {} to {}".format(last_processed_input_id, last_processed_input_id + processing_row_count))
        multi_address__clustering_heuristic()

        # close arangodb connection
        close_gp_connection()

    # save wallet maps in file system
    save_wallet_data()

    # clear all loaded in-memory data
    clear_data()

    # print_wallet_data_structure()
    print("Multi address clustering process completed successfully")


if __name__ == "__main__":
    main()
