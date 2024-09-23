create schema if not exists iceberg.staging_vault;
use iceberg.staging_vault;

-- summary
call iceberg.system.register_table(
    schema_name => 'staging_vault',
    table_name => 'summary',
    table_location => 's3a://warehouse/staging_vault/summary'
    );

create table if not exists hub_summary
with (
    format = 'parquet',
    location = 's3a://warehouse/raw_vault/hub_summary'
) as
select exchange, indexId, time, timeMaker, label, exchangeLabel, ETL_time
from summary;

create table if not exists sat_summary
with (
    format = 'parquet',
    location = 's3a://warehouse/raw_vault/sat_summary'
) as
select *
from summary;

-- group
call iceberg.system.register_table(
    schema_name => 'staging_vault',
    table_name => 'group',
    table_location => 's3a://warehouse/staging_vault/group'
    );

create table if not exists hub_group
with (
    format = 'parquet',
    location = 's3a://warehouse/raw_vault/hub_group'
) as
select sn, e as exchange, ss, indexId, ETL_time, td
from group;

create table if not exists sat_group
with (
    format = 'parquet',
    location = 's3a://warehouse/raw_vault/sat_group'
) as
select *
from group;

-- exchange_index
call iceberg.system.register_table(
    schema_name => 'staging_vault',
    table_name => 'exchange_index',
    table_location => 's3a://warehouse/staging_vault/exchange_index'
    );

create table if not exists hub_exchange_index
with (
    format = 'parquet',
    location = 's3a://warehouse/raw_vault/hub_exchange_index'
) as
select indexId, time, ETL_time, exchange
from exchange_index;

create table if not exists sat_exchange_index
with (
    format = 'parquet',
    location = 's3a://warehouse/raw_vault/sat_exchange_index'
) as
select *
from exchange_index;

-- odd_exchange
call iceberg.system.register_table(
    schema_name => 'staging_vault',
    table_name => 'odd_exchange',
    table_location => 's3a://warehouse/staging_vault/odd_exchange'
    );

create table if not exists hub_odd_exchange
with (
    format = 'parquet',
    location = 's3a://warehouse/raw_vault/hub_odd_exchange'
) as
select sn, e as exchange, ss, ETL_time, ltu, td
from odd_exchange;

create table if not exists sat_odd_exchange
with (
    format = 'parquet',
    location = 's3a://warehouse/raw_vault/sat_odd_exchange'
) as
select *
from odd_exchange;

-- put_exec
call iceberg.system.register_table(
    schema_name => 'staging_vault',
    table_name => 'put_exec',
    table_location => 's3a://warehouse/staging_vault/put_exec'
    );

create table if not exists hub_put_exec
with (
    format = 'parquet',
    location = 's3a://warehouse/raw_vault/hub_put_exec'
) as
select _id, stockSymbol, exchange, createAt, ETL_time
from put_exec;

create table if not exists sat_put_exec
with (
    format = 'parquet',
    location = 's3a://warehouse/raw_vault/sat_put_exec'
) as
select *
from put_exec;

-- link
create table if not exists link_summary_ei
with (
    format = 'parquet',
    location = 's3a://warehouse/raw_vault/link_summary_ei'
) as
select exchange, indexId, s.time as summaryTime, timeMaker, label, exchangeLabel, ETL_time, ei.time as eiTime
from summary as s, exchange_index as ei
where s.exchange = ei.exchange and s.indexId = ei.indexId and s.ETL_time = ei.ETL_time;

create table if not exists link_summary_group
with (
    format = 'parquet',
    location = 's3a://warehouse/raw_vault/link_summary_group'
) as
select exchange, indexId, time, timeMaker, label, exchangeLabel, ETL_time, sn, ss, td
from summary as s, group as g
where s.exchange = g.exchange and s.indexId = g.indexId and s.ETL_time = g.ETL_time;

create table if not exists link_group_oe
with (
    format = 'parquet',
    location = 's3a://warehouse/raw_vault/link_group_oe'
) as
select sn, exchange, ss, indexId, ETL_time, g.td as groupTd, ltu, oe.td as oeTd
from group as g, odd_exchange as oe
where g.sn = oe.sn and g.exchange = oe.exchange and g.ss = oe.ss and g.ETL_time = oe.ETL_time