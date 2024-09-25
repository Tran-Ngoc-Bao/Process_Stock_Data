create schema if not exists iceberg.star_schema_kimball;
use iceberg.star_schema_kimball;

create table if not exists fact_summary_ei_group
with (format = 'parquet')
as select *
from iceberg.raw_vault.link_summary_ei_group;

create table if not exists dim_summary
with (format = 'parquet')
as select *
from iceberg.raw_vault.sat_summary;

create table if not exists dim_exchange_index
with (format = 'parquet')
as select *
from iceberg.raw_vault.sat_exchange_index;

create table if not exists dim_group_buy
with (format = 'parquet')
as select *
from iceberg.raw_vault.sat_group_buy;

create table if not exists dim_group_sell
with (format = 'parquet')
as select *
from iceberg.raw_vault.sat_group_sell;

create table if not exists dim_group_khop_lenh
with (format = 'parquet')
as select *
from iceberg.raw_vault.sat_group_khop_lenh;

create table if not exists dim_group_dtnn
with (format = 'parquet')
as select *
from iceberg.raw_vault.sat_group_dtnn;

create table if not exists dim_group_summary
with (format = 'parquet')
as select *
from iceberg.raw_vault.sat_group_summary;

create table if not exists dim_group_other
with (format = 'parquet')
as select *
from iceberg.raw_vault.sat_group_other;

insert into fact_summary_ei_group
select *
from iceberg.raw_vault.link_summary_ei_group
where load_date = (select max(load_date) from iceberg.business_vault.pit_summary);

insert into dim_summary
select *
from iceberg.raw_vault.sat_summary
where load_date = (select max(load_date) from iceberg.business_vault.pit_summary);

insert into dim_exchange_index
select *
from iceberg.raw_vault.sat_exchange_index
where load_date = (select max(load_date) from iceberg.business_vault.pit_exchange_index);

insert into dim_group_buy
select *
from iceberg.raw_vault.sat_group_buy
where load_date = (select max(load_date) from iceberg.business_vault.pit_group);

insert into dim_group_sell
select *
from iceberg.raw_vault.sat_group_sell
where load_date = (select max(load_date) from iceberg.business_vault.pit_group);

insert into dim_group_khop_lenh
select *
from iceberg.raw_vault.sat_group_khop_lenh
where load_date = (select max(load_date) from iceberg.business_vault.pit_group);

insert into dim_group_dtnn
select *
from iceberg.raw_vault.sat_group_dtnn
where load_date = (select max(load_date) from iceberg.business_vault.pit_group);

insert into dim_group_summary
select *
from iceberg.raw_vault.sat_group_summary
where load_date = (select max(load_date) from iceberg.business_vault.pit_group);

insert into dim_group_other
select *
from iceberg.raw_vault.sat_group_other
where load_date = (select max(load_date) from iceberg.business_vault.pit_group);