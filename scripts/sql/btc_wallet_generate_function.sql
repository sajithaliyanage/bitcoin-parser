
-- This function responsible for populate table with values
create or replace function generate_wallets()
returns void
as $$
begin 
    PERFORM create_wallets();
    raise notice 'Wallet tabel creation is complete!';
    PERFORM enrich_wallet_total_spent();
    raise notice 'Wallet tabel is updated with total spent amounts!';
    PERFORM enrich_wallet_total_received();
    raise notice 'Wallet tabel is updated with total received amounts!';
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
	    num_address integer,
	    num_tx integer,
	    total_spent bigint,
	    total_received bigint
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
        select cluster_id, SUM(tx_count) as tx_count from (
          select cluster_id, clusters.address, tx_count from (
          select address, count(distinct tx_hash) as tx_count from (
          select address, tx_hash from btc_tx_input 
          UNION 
          select address, tx_hash from btc_tx_output) 
          as each_address group by address) as transactions 
          INNER JOIN btc_address_cluster as clusters 
          ON transactions.address = clusters.address) as total_tx_count 
      group by cluster_id
      ) AS total_tx_count_table
    WHERE
      btc_wallet.cluster_id=total_tx_count_table.cluster_id;
end ;
$$ language plpgsql;
