create schema iceberg.raw_vault;
use iceberg.raw_vault;

-- summary
call iceberg.system.register_table(
    schema_name => 'raw_vault',
    table_name => 'summary',
    table_location => 's3a://warehouse/staging_vault/summary'
    );

create table hub_summary
with (
    format = 'parquet',
    location = 's3a://warehouse/raw_vault/hub_summary'
) as
select sha1(cast(concat(exchange, indexId, time, timeMaker, label, exchangeLabel, ETL_time) as varbinary)) as hub_summary_hash_key, 
ETL_time as load_date, indexId as record_source
from summary;

create table sat_summary
with (
    format = 'parquet',
    location = 's3a://warehouse/raw_vault/sat_summary'
) as
select sha1(cast(concat(exchange, indexId, time, timeMaker, label, exchangeLabel, ETL_time) as varbinary)) as hub_summary_hash_key,
sha1(cast(concat(indexValue, prevIndexValue, allQty, allValue, change, changePercent) as varbinary)) as hash_diff,
ETL_time as load_date, indexId as record_source, *
from summary;

-- group
call iceberg.system.register_table(
    schema_name => 'raw_vault',
    table_name => 'gr',
    table_location => 's3a://warehouse/staging_vault/group'
    );

create table hub_group
with (
    format = 'parquet',
    location = 's3a://warehouse/raw_vault/hub_group'
) as
select sha1(cast(concat(sn, e as exchange, ss, indexId, ETL_time, td) as varbinary)) as hub_group_hash_key,
ETL_time as load_date, indexId as record_source
from gr;

create table sat_group
with (
    format = 'parquet',
    location = 's3a://warehouse/raw_vault/sat_group'
) as
select sha1(cast(concat(sn, e as exchange, ss, indexId, ETL_time, td) as varbinary)) as hub_group_hash_key,
sha1(cast(concat(st, tu, s, ce, cv) as varbinary)) as hash_diff,
ETL_time as load_date, indexId as record_source, *
from gr;

-- exchange_index
call iceberg.system.register_table(
    schema_name => 'raw_vault',
    table_name => 'exchange_index',
    table_location => 's3a://warehouse/staging_vault/exchange_index'
    );

create table hub_exchange_index
with (
    format = 'parquet',
    location = 's3a://warehouse/raw_vault/hub_exchange_index'
) as
select sha1(cast(concat(indexId, time, ETL_time, exchange) as varbinary)) as hub_exchange_index_hash_key,
ETL_time as load_date, indexId as record_source
from exchange_index;

create table sat_exchange_index
with (
    format = 'parquet',
    location = 's3a://warehouse/raw_vault/sat_exchange_index'
) as
select sha1(cast(concat(indexId, time, ETL_time, exchange) as varbinary)) as hub_exchange_index_hash_key,
sha1(cast(concat(indexValue, vol, totalQtty)) as varbinary) as hash_diff,
ETL_time as load_date, indexId as record_source, *
from exchange_index;

-- odd_exchange
call iceberg.system.register_table(
    schema_name => 'raw_vault',
    table_name => 'odd_exchange',
    table_location => 's3a://warehouse/staging_vault/odd_exchange'
    );

create table hub_odd_exchange
with (
    format = 'parquet',
    location = 's3a://warehouse/raw_vault/hub_odd_exchange'
) as
select sha1(cast(concat(sn, e as exchange, ss, ETL_time, ltu, td) as varbinary)) as hub_odd_exchange_hash_key,
ETL_time as load_date, exchange as record_source
from odd_exchange;

create table sat_odd_exchange
with (
    format = 'parquet',
    location = 's3a://warehouse/raw_vault/sat_odd_exchange'
) as
select sha1(cast(concat(sn, e as exchange, ss, ETL_time, ltu, td) as varbinary)) as hub_odd_exchange_hash_key,
sha1(cast(concat(st, tu, s, ce, cv) as varbinary)) as hash_diff,
ETL_time as load_date, exchange as record_source, *
from odd_exchange;

-- put_exec
call iceberg.system.register_table(
    schema_name => 'raw_vault',
    table_name => 'put_exec',
    table_location => 's3a://warehouse/staging_vault/put_exec'
    );

create table hub_put_exec
with (
    format = 'parquet',
    location = 's3a://warehouse/raw_vault/hub_put_exec'
) as
select sha1(cast(concat(_id, stockSymbol, exchange, createdAt, ETL_time) as varbinary)) as hub_put_exec_hash_key,
ETL_time as load_date, exchange as record_source
from put_exec;

create table sat_put_exec
with (
    format = 'parquet',
    location = 's3a://warehouse/raw_vault/sat_put_exec'
) as
select sha1(cast(concat(_id, stockSymbol, exchange, createdAt, ETL_time) as varbinary)) as hub_put_exec_hash_key,
sha1(cast(concat(refPrice, ceiling, floor, vol, val, price, ptTotalTradedQty, ptTotalTradedValue) as varbinary)) as hash_diff,
ETL_time as load_date, exchange as record_source, *
from put_exec;

-- link
create table link_summary_ei
with (
    format = 'parquet',
    location = 's3a://warehouse/raw_vault/link_summary_ei'
) as
select sha1(concat(hub_summary_hash_key, hub_exchange_index_hash_key)) as link_summary_ei_hash_key,
hub_summary_hash_key, hub_exchange_index, s.load_date, s.record_source
from hub_summary as s, hub_exchange_index as ei
where s.load_date = ei.load_date and s.record_source = ei.record_source;

create table link_summary_group
with (
    format = 'parquet',
    location = 's3a://warehouse/raw_vault/link_summary_group'
) as
select sha1(concat(hub_summary_hash_key, hub_group_hash_key)) as link_summary_group_hash_key,
hub_summary_hash_key, hub_group_hash_key, s.load_date, s.record_source
from hub_summary as s, hub_group as g
where s.load_date = g.load_date and s.record_source = g.record_source;

create table link_oe_pe
with (
    format = 'parquet',
    location = 's3a://warehouse/raw_vault/link_oe_pe'
) as
select sha1(concat(hub_odd_exchange_hash_key, hub_put_exec_hash_key)) as link_oe_pe_hash_key
hub_odd_exchange_hash_key, hub_put_exec_hash_key, oe.load_date, oe.record_source
from hub_odd_exchange as oe, hub_put_exec as pe
where oe.load_date = pe.load_date and oe.record_source = pe.record_source;

create schema iceberg.business_vault;
use iceberg.business_vault;

-- PIT
create table pit_summary
with (
    format = 'parquet',
    location = 's3a://warehouse/business_vault/pit_summary'
) as
select hub_summary_hash_key, load_date, time, timeMaker, ETL_time
from iceberg.raw_vault.sat_summary;

create table pit_group
with (
    format = 'parquet',
    location = 's3a://warehouse/business_vault/pit_group'
) as
select hub_group_hash_key, load_date, td, ETL_time
from iceberg.raw_vault.sat_group;

create table pit_exchange_index
with (
    format = 'parquet',
    location = 's3a://warehouse/business_vault/pit_exchange_index'
) as
select hub_exchange_index_hash_key, load_date, time, ETL_time
from iceberg.raw_vault.sat_exchange_index;

create table pit_odd_exchange
with (
    format = 'parquet',
    location = 's3a://warehouse/business_vault/pit_odd_exchange'
) as
select hub_odd_exchange_hash_key, load_date, isd, ltu, td, ETL_time
from iceberg.raw_vault.sat_odd_exchange;

create table pit_put_exec
with (
    format = 'parquet',
    location = 's3a://warehouse/business_vault/pit_put_exec'
) as
select hub_put_exec_hash_key, load_date, createdAt, ETL_time
from iceberg.raw_vault.sat_put_exec;