create or replace function wallets_for_missing_outputs()
returns void
as $$
begin 
    insert into btc_address_cluster(cluster_id, address) (
      SELECT (uuid_in(overlay(overlay(md5(random()::text || ':' || clock_timestamp()::text) placing '4' from 13) placing to_hex(floor(random()*(11-8+1) + 8)::int)::text from 17)::cstring)) as cluster_id, address from (
      SELECT distinct address FROM btc_tx_output WHERE NOT EXISTS(SELECT address FROM btc_address_cluster WHERE btc_address_cluster.address=btc_tx_output.address)) as missing_address
    );
end ;
$$ language plpgsql;