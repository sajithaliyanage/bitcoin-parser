
-- This function responsible for populate table with values
create or replace function generate_wallets()
returns void
as $$
begin 
    PERFORM create_wallets_money_flow();
    raise notice 'Wallet money flow tabel creation is complete!';
    PERFORM enrich_wallet_money_inflow();
    raise notice 'Wallet money flow tabel is updated with inflows!';
    PERFORM enrich_wallet_money_outflow();
    raise notice 'Wallet money flow tabel is updated with outflows!';
end ;
$$ language plpgsql;


-- This function responsible for creating btc_wallet_money_flow table
create or replace function create_wallets_money_flow()
returns void
as $$
begin
    DROP TABLE IF EXISTS btc_wallet_money_flow;
    CREATE TABLE btc_wallet_money_flow(
      id SERIAL primary key NOT NULL,
      wallet_id varchar(65),
      num_address varchar(65),
      category varchar(255),
      total_amount bigint,
      total_usd_amount numeric,
      flow_type varchar(10)
    );
end ;
$$ language plpgsql;


create or replace function enrich_wallet_money_outflow()
returns void
as $$
begin 
    insert into btc_wallet_money_flow(wallet_id, num_address, category, total_amount, total_usd_amount, flow_type) (
      select wallet_id, count(distinct address) as num_address, category, SUM(tx_value) as total_amount, SUM(usd_value) as total_usd_amount, 'OUT' as flow_type from (
      select cluster_id as wallet_id, cluster_address.address, category, tx_value, usd_value from btc_tx_input join (
      select cluster_id, btc_address_cluster.address, category from btc_address_cluster 
      join btc_address_label on btc_address_cluster.address = btc_address_label.address
      ) as cluster_address on cluster_address.address=btc_tx_input.address
      ) as money_inflow group by wallet_id, category
    );
end ;
$$ language plpgsql;


create or replace function enrich_wallet_money_inflow()
returns void
as $$
begin 
    insert into btc_wallet_money_flow(wallet_id, num_address, category, total_amount, total_usd_amount, flow_type) (
      select wallet_id, count(distinct address) as num_address, category, SUM(tx_value) as total_amount, SUM(usd_value) as total_usd_amount, 'IN' as flow_type from (
      select cluster_id as wallet_id, cluster_address.address, category, tx_value, usd_value from btc_tx_output join (
      select cluster_id, btc_address_cluster.address, category from btc_address_cluster 
      join btc_address_label on btc_address_cluster.address = btc_address_label.address
      ) as cluster_address on cluster_address.address=btc_tx_output.address
      ) as money_inflow group by wallet_id, category
    ); 
end ;
$$ language plpgsql;
