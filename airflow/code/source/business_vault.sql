use iceberg.business_vault;

-- PIT
insert into pit_summary
select
hub_summary_hash_key, load_date, time, timeMaker, ETL_time
from iceberg.raw_vault.sat_summary
where load_date = (select max(load_date) from iceberg.raw_vault.sat_summary);

insert into pit_group
select
hub_group_hash_key, load_date, td, ETL_time
from iceberg.raw_vault.sat_group
where load_date = (select max(load_date) from iceberg.raw_vault.sat_group);

insert into pit_exchange_index
select
hub_exchange_index_hash_key, load_date, time, ETL_time
from iceberg.raw_vault.sat_exchange_index
where load_date = (select max(load_date) from iceberg.raw_vault.sat_exchange_index);

insert into pit_odd_exchange
select
hub_odd_exchange_hash_key, load_date, isd, ltu, td, ETL_time
from iceberg.raw_vault.sat_odd_exchange
where load_date = (select max(load_date) from iceberg.raw_vault.sat_odd_exchange);

insert into pit_put_exec
select
hub_put_exec_hash_key, load_date, createdAt, ETL_time
from iceberg.raw_vault.sat_put_exec
where load_date = (select max(load_date) from iceberg.raw_vault.sat_put_exec);

-- bridge
insert into bridge_summary_ei_group
select
link_summary_ei_hash_key, link_summary_group_hash_key,
sei.hub_summary_hash_key, sei.hub_exchange_index_hash_key, sg.hub_group_hash_key,
sei.load_date, sei.record_source
from iceberg.raw_vault.link_summary_ei as sei, iceberg.raw_vault.link_summary_group as sg
where sei.load_date = (select max(load_date) from pit_summary)
and sei.hub_summary_hash_key = sg.hub_summary_hash_key;
