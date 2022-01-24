CREATE DATABASE btc_blockchain;

\c  btc_blockchain;

CREATE TABLE btc_transaction (
	id integer SERIAL,
	fee bigint,
	block_number integer,
	input_value bigint DEFAULT '0',
	index integer SERIAL,
	is_coinbase boolean,
	output_count integer,
	output_value bigint,
	hash varchar(65),
	input_count integer
);

CREATE TABLE btc_block (
	id integer SERIAL,
    height integer,
	hash varchar(65),
    block_time varchar(16),
    tx_count integer
);

CREATE TABLE btc_tx_input (
	id integer SERIAL,
	tx_hash varchar(65),
	index integer,
	required_signatures integer,
    spent_output_index int,
    spent_tx_hash varchar(65),
	address_type varchar(16),
	tx_value bigint,
	address varchar(65)
);

CREATE TABLE btc_tx_output (
	id integer SERIAL,
	tx_hash varchar(65),
	index integer,
	required_signatures integer,
	address_type varchar(16),
	tx_value bigint,
	address varchar(65)
);
