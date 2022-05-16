-- This function responsible for populate table with values
create
or replace function enrich_wallet_addresses() returns void as $ $ begin DROP INDEX wallet_address_index;

DROP INDEX wallet_id_index;

PERFORM enrich_wallet_address_spent_amounts();

raise notice 'Address cluster tabel is updated with total spent amounts!';

PERFORM enrich_wallet_address_received_amounts();

raise notice 'Address cluster is updated with total spent usd amounts!';

PERFORM enrich_wallet_address_labels();

raise notice 'Address cluster is updated with labels!';

CREATE INDEX wallet_address_index ON btc_address_cluster(address);

CREATE INDEX wallet_id_index ON btc_address_cluster(cluster_id);

raise notice 'Indexes created for table btc_address_cluster!';

end;

$ $ language plpgsql;

create
or replace function enrich_wallet_address_spent_amounts() returns void as $ $ begin DROP TABLE IF EXISTS btc_wallet_address_spent;

CREATE TABLE btc_wallet_address_spent (
  id integer primary key NOT NULL,
  total_spent_satoshi bigint DEFAULT 0,
  total_spent_usd numeric DEFAULT 0
);

raise notice 'Temporary btc_wallet_address_spent table created!';

insert into
  btc_wallet_address_spent(id, total_spent_satoshi, total_spent_usd) (
    select
      btc_address_cluster.id,
      sum(tx_value) as total_spent_satoshi,
      sum(usd_value) as total_spent_usd
    from
      btc_address_cluster
      INNER JOIN btc_tx_input ON btc_address_cluster.address = btc_tx_input.address
    group by
      btc_address_cluster.id
  );

raise notice 'btc_wallet_address_spent table filled with data!';

UPDATE
  btc_address_cluster
SET
  total_spent_satoshi = btc_wallet_address_spent.total_spent_satoshi,
  total_spent_usd = btc_wallet_address_spent.total_spent_usd
FROM
  btc_wallet_address_spent
WHERE
  btc_address_cluster.id = btc_wallet_address_spent.id;

raise notice 'btc_address_cluster is updated with btc_wallet_address_spent data!';

DROP TABLE IF EXISTS btc_wallet_address_spent;

end;

$ $ language plpgsql;

create
or replace function enrich_wallet_address_received_amounts() returns void as $ $ begin DROP TABLE IF EXISTS btc_wallet_address_received;

CREATE TABLE btc_wallet_address_received (
  id integer primary key,
  total_received_satoshi bigint DEFAULT 0,
  total_received_usd numeric DEFAULT 0
);

raise notice 'Temporary btc_wallet_address_received table created!';

insert into
  btc_wallet_address_received(id, total_received_satoshi, total_received_usd) (
    select
      btc_address_cluster.id,
      sum(tx_value) as total_received_satoshi,
      sum(usd_value) as total_received_usd
    from
      btc_address_cluster
      INNER JOIN btc_tx_output ON btc_address_cluster.address = btc_tx_output.address
    group by
      btc_address_cluster.id
  );

raise notice 'btc_wallet_address_received table filled with data!';

UPDATE
  btc_address_cluster
SET
  total_received_satoshi = btc_wallet_address_received.total_received_satoshi,
  total_received_usd = btc_wallet_address_received.total_received_usd
FROM
  btc_wallet_address_received
WHERE
  btc_address_cluster.id = btc_wallet_address_received.id;

raise notice 'btc_address_cluster is updated with btc_wallet_address_received data!';

DROP TABLE IF EXISTS btc_wallet_address_received;

end;

$ $ language plpgsql;

create
or replace function enrich_wallet_address_labels() returns void as $ $ begin DROP TABLE IF EXISTS btc_wallet_address_label;

CREATE TABLE btc_wallet_address_label (
  id integer primary key,
  label text,
  source text
);

raise notice 'Temporary btc_wallet_address_label table created!';

insert into
  btc_wallet_address_label(id, label, source) (
    SELECT
      btc_address_cluster.id,
      array_to_string(array_agg(btc_address_label.label), ',') as label,
      array_to_string(array_agg(btc_address_label.source), ',') as source
    FROM
      btc_address_label
      join btc_address_cluster ON btc_address_cluster.address = btc_address_label.address
    GROUP BY
      btc_address_cluster.id
  );

raise notice 'btc_wallet_address_label table filled with data!';

UPDATE
  btc_address_cluster
SET
  label = btc_wallet_address_label.label,
  source = btc_wallet_address_label.source
FROM
  btc_wallet_address_label
WHERE
  btc_address_cluster.id = btc_wallet_address_label.id;

raise notice 'btc_address_cluster is updated with btc_wallet_address_label data!';

DROP TABLE IF EXISTS btc_wallet_address_label;

end;

$ $ language plpgsql;

create
or replace function create_wallet_transactions() returns void as $ $ begin DROP TABLE IF EXISTS btc_wallet_transaction;

CREATE TABLE btc_wallet_transaction(
  id SERIAL primary key NOT NULL,
  cluster_id varchar(100),
  tx_hash varchar(65),
  block_number integer,
  input_value bigint,
  output_value bigint,
  is_coinbase boolean,
  input_count integer,
  output_count integer,
  tx_type varchar(40),
  input_usd_value numeric,
  output_usd_value numeric
);

raise notice 'btc_wallet_transaction table created!';

insert into
  btc_wallet_transaction(
    cluster_id,
    tx_hash,
    block_number,
    input_value,
    output_value,
    is_coinbase,
    input_count,
    output_count,
    tx_type
  ) (
    select
      cluster_id,
      wallet_txes.tx_hash,
      block_number,
      input_value,
      output_value,
      is_coinbase,
      input_count,
      output_count,
      'Sending' as tx_type
    from
      (
        select
          distinct tx_hash,
          cluster_id
        from
          btc_address_cluster
          inner join btc_tx_input on btc_address_cluster.address = btc_tx_input.address
        group by
          cluster_id,
          tx_hash
      ) as wallet_txes
      inner join btc_transaction ON wallet_txes.tx_hash = btc_transaction.hash
  );

raise notice 'btc_wallet_transaction table filled with spent data!';

insert into
  btc_wallet_transaction(
    cluster_id,
    tx_hash,
    block_number,
    input_value,
    output_value,
    is_coinbase,
    input_count,
    output_count,
    tx_type
  ) (
    select
      cluster_id,
      wallet_txes.tx_hash,
      block_number,
      input_value,
      output_value,
      is_coinbase,
      input_count,
      output_count,
      'Receiving' as tx_type
    from
      (
        select
          distinct tx_hash,
          cluster_id
        from
          (
            select
              cluster_id,
              address
            from
              btc_address_cluster
          ) as address_cluster
          inner join btc_tx_output on address_cluster.address = btc_tx_output.address
        group by
          cluster_id,
          tx_hash
      ) as wallet_txes
      inner join btc_transaction ON wallet_txes.tx_hash = btc_transaction.hash
  );

raise notice 'btc_wallet_transaction table filled with received data!';

end;

$ $ language plpgsql;