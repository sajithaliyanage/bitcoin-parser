
-- This function responsible for populate table with values
create or replace function generate_wallets()
returns void
as $$
begin 
    PERFORM create_wallets();
    raise notice 'Wallet tabel creation is complete!';
    PERFORM enrich_wallet_total_spent();
    raise notice 'Wallet tabel is updated with total spent amounts!';
    PERFORM enrich_wallet_total_spent_usd();
    raise notice 'Wallet tabel is updated with total spent usd amounts!';
    PERFORM enrich_wallet_total_received();
    raise notice 'Wallet tabel is updated with total received amounts!';
    PERFORM enrich_wallet_total_received_usd();
    raise notice 'Wallet tabel is updated with total received usd amounts!';
    PERFORM enrich_wallet_total_tx();
    raise notice 'Wallet tabel is updated with total tx counts!';
end ;
$$ language plpgsql;

-- This function responsible for creating btc_wallet table with values
create or replace function create_wallets()
returns void
as $$
begin 
    DROP TABLE IF EXISTS btc_wallet;
    CREATE TABLE btc_wallet (
	    id SERIAL primary key NOT NULL,
	    cluster_id varchar(65),
	    num_address integer DEFAULT 0,
	    num_tx integer DEFAULT 0,
	    total_spent bigint DEFAULT 0,
      total_spent_usd numeric DEFAULT 0,
	    total_received bigint DEFAULT 0,
      total_received_usd numeric DEFAULT 0,
      risk_score float DEFAULT 0,
      label text,
      label_source text,
      category text
    );
    insert into btc_wallet(cluster_id, num_address) (select cluster_id, count(address) as total_address from btc_address_cluster group by cluster_id);
end ;
$$ language plpgsql;

create or replace function enrich_wallet_total_spent()
returns void
as $$
begin 
    UPDATE
      btc_wallet
    SET
      total_spent=total_spent_table.total_spent
    FROM
      (
        select * from (
            select SUM(total_value) as total_spent, cluster_id from (
            select all_inputs.address, total_value, cluster_id from (
            select address, SUM(tx_value) as total_value from btc_tx_input group by address) AS all_inputs 
            INNER JOIN (select address, cluster_id from btc_address_cluster) as cluster 
            ON cluster.address = all_inputs.address) as total_spent 
            group by cluster_id) as final
      ) AS total_spent_table
    WHERE
      btc_wallet.cluster_id=total_spent_table.cluster_id;
end ;
$$ language plpgsql;

create or replace function enrich_wallet_total_spent_usd()
returns void
as $$
begin 
    UPDATE
      btc_wallet
    SET
      total_spent_usd=total_spent_usd_table.total_spent_usd
    FROM
      (
        select * from (
            select SUM(total_usd_value) as total_spent_usd, cluster_id from (
            select all_inputs.address, total_usd_value, cluster_id from (
            select address, SUM(usd_value) as total_usd_value from btc_tx_input group by address) AS all_inputs 
            INNER JOIN (select address, cluster_id from btc_address_cluster) as cluster 
            ON cluster.address = all_inputs.address) as total_spent_usd 
            group by cluster_id) as final
      ) AS total_spent_usd_table
    WHERE
      btc_wallet.cluster_id=total_spent_usd_table.cluster_id;
end ;
$$ language plpgsql;

create or replace function enrich_wallet_total_received()
returns void
as $$
begin 
    UPDATE
      btc_wallet
    SET
      total_received=total_received_table.total_received
    FROM
      (
        select * from (
          select SUM(total_value) as total_received, cluster_id from (
          select all_outputs.address, total_value, cluster_id from (
          select address, SUM(tx_value) as total_value from btc_tx_output group by address) AS all_outputs 
          INNER JOIN (select address, cluster_id from btc_address_cluster) as cluster 
          ON cluster.address = all_outputs.address) as total_received 
        group by cluster_id) as final
      ) AS total_received_table
    WHERE
      btc_wallet.cluster_id=total_received_table.cluster_id;
end ;
$$ language plpgsql;

create or replace function enrich_wallet_total_received_usd()
returns void
as $$
begin 
    UPDATE
      btc_wallet
    SET
      total_received_usd=total_received_usd_table.total_received_usd
    FROM
      (
        select * from (
          select SUM(total_usd_value) as total_received_usd, cluster_id from (
          select all_outputs.address, total_usd_value, cluster_id from (
          select address, SUM(usd_value) as total_usd_value from btc_tx_output group by address) AS all_outputs 
          INNER JOIN (select address, cluster_id from btc_address_cluster) as cluster 
          ON cluster.address = all_outputs.address) as total_received 
        group by cluster_id) as final
      ) AS total_received_usd_table
    WHERE
      btc_wallet.cluster_id=total_received_usd_table.cluster_id;
end ;
$$ language plpgsql;

create or replace function enrich_wallet_total_tx()
returns void
as $$
begin 
    UPDATE
      btc_wallet
    SET
      num_tx=total_tx_count_table.tx_count
    FROM
      (
        SELECT cluster_id, count(DISTINCT tx_hash) AS tx_count FROM btc_address_cluster JOIN (
          SELECT address, tx_hash FROM ( 
            SELECT address,tx_hash FROM btc_tx_input 
            UNION 
            SELECT address,tx_hash FROM btc_tx_output
            ) as addresses
          ) as txes 
        ON btc_address_cluster.address=txes.address GROUP BY cluster_id
      ) AS total_tx_count_table
    WHERE
      btc_wallet.cluster_id=total_tx_count_table.cluster_id;
end ;
$$ language plpgsql;

create or replace function enrich_wallet_labels_list()
returns void
as $$
begin 
    UPDATE
      btc_wallet
    SET
      label=label_wallets.labels,
      label_source=label_wallets.sources,
      category=label_wallets.categories
    FROM
      (
        SELECT cluster_id, array_to_string(array_agg(labels), ',') as labels, array_to_string(array_agg(sources), ',') as sources, array_to_string(array_agg(categories), ',') as categories from btc_address_cluster inner join (
        SELECT address, array_to_string(array_agg(label), ',') as labels, array_to_string(array_agg(source), ',') as sources, array_to_string(array_agg(category), ',') as categories from btc_address_label group by address
        ) as label_address on label_address.address=btc_address_cluster.address group by cluster_id
      ) AS label_wallets
    WHERE
      btc_wallet.cluster_id=label_wallets.cluster_id;
end ;
$$ language plpgsql;

create or replace function enrich_wallet_labels()
returns void
as $$
begin 
    UPDATE
      btc_wallet
    SET
      labels=label_wallets.labels
    FROM
      (
        SELECT id, CAST(json_object_agg(element, count) AS TEXT) as json from (
        with elements (id, element) as (select id, unnest(string_to_array(label, ',')) from btc_wallet 
        group by id) select id, element, count(*) as count 
        from elements group by id,element order by count desc) as data group by id
      ) AS label_wallets
    WHERE
      btc_wallet.id=label_wallets.id;
end ;
$$ language plpgsql;

create or replace function enrich_wallet_categories()
returns void
as $$
begin 
    UPDATE
      btc_wallet
    SET
      categories=label_wallets.json
    FROM
      (
        SELECT id, CAST(json_object_agg(element, count) AS TEXT) as json from (
        with elements (id, element) as (select id, unnest(string_to_array(label, ',')) from btc_wallet 
        group by id) select id, element, count(*) as count 
        from elements group by id,element order by count desc) as data group by id
      ) AS label_wallets
    WHERE
      btc_wallet.id=label_wallets.id;
end ;
$$ language plpgsql;
