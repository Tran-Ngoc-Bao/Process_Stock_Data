create schema if not exists iceberg.business_vault;
use iceberg.business_vault;

-- PIT
create table if not exists pit_summary
with (format = 'parquet')
as select
hub_summary_hash_key, record_source, load_date, time, timeMaker, ETL_time
from iceberg.raw_vault.sat_summary;

create table if not exists pit_group
with (format = 'parquet')
as select
hub_group_hash_key, record_source, load_date
from iceberg.raw_vault.hub_group;

create table if not exists pit_exchange_index
with (format = 'parquet')
as select
hub_exchange_index_hash_key, record_source, load_date, time, ETL_time
from iceberg.raw_vault.sat_exchange_index;

-- PIT
insert into pit_summary
select
hub_summary_hash_key, record_source, load_date, time, timeMaker, ETL_time
from iceberg.raw_vault.sat_summary
where load_date = (select max(load_date) from iceberg.raw_vault.sat_summary);

insert into pit_group
select
hub_group_hash_key, record_source, load_date
from iceberg.raw_vault.hub_group
where load_date = (select max(load_date) from iceberg.raw_vault.hub_group);

insert into pit_exchange_index
select
hub_exchange_index_hash_key, record_source, load_date, time, ETL_time
from iceberg.raw_vault.sat_exchange_index
where load_date = (select max(load_date) from iceberg.raw_vault.sat_exchange_index);
