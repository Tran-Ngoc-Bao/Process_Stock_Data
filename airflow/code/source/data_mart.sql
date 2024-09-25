create schema if not exists iceberg.data_mart;
use iceberg.data_mart;

create table if not exists history
as select *
from iceberg.star_schema_kimball.dim_exchange_index;

insert into history
select *
from iceberg.star_schema_kimball.dim_exchange_index
where load_date = (select max(load_date) from iceberg.business_vault.pit_exchange_index);

create table if not exists history_summary
as select *
from iceberg.star_schema_kimball.dim_summary;

insert into history_summary
select *
from iceberg.star_schema_kimball.dim_summary
where load_date = (select max(load_date) from iceberg.business_vault.pit_summary);

create table if not exists buy
as select *
from iceberg.star_schema_kimball.dim_group_buy b, iceberg.star_schema_kimball.dim_group_summary s
where b.hub_group_hash_key = s.hub_group_hash_key;

create table if not exists sell
as select *
from iceberg.star_schema_kimball.dim_group_sell b, iceberg.star_schema_kimball.dim_group_summary s
where b.hub_group_hash_key = s.hub_group_hash_key;

create table if not exists khop_lenh
as select *
from iceberg.star_schema_kimball.dim_group_khop_lenh b, iceberg.star_schema_kimball.dim_group_summary s
where b.hub_group_hash_key = s.hub_group_hash_key;

create table if not exists dtnn
as select *
from iceberg.star_schema_kimball.dim_group_dtnn b, iceberg.star_schema_kimball.dim_group_summary s
where b.hub_group_hash_key = s.hub_group_hash_key;

insert into buy
select *
from iceberg.star_schema_kimball.dim_group_buy b, iceberg.star_schema_kimball.dim_group_summary s
where b.load_date = (select max(load_date) from iceberg.business_vault.pit_group) and b.hub_group_hash_key = s.hub_group_hash_key;

insert into sell
select *
from iceberg.star_schema_kimball.dim_group_sell b, iceberg.star_schema_kimball.dim_group_summary s
where b.load_date = (select max(load_date) from iceberg.business_vault.pit_group) and b.hub_group_hash_key = s.hub_group_hash_key;

insert into khop_lenh
select *
from iceberg.star_schema_kimball.dim_group_khop_lenh b, iceberg.star_schema_kimball.dim_group_summary s
where b.load_date = (select max(load_date) from iceberg.business_vault.pit_group) and b.hub_group_hash_key = s.hub_group_hash_key;

insert into dtnn
select *
from iceberg.star_schema_kimball.dim_group_dtnn b, iceberg.star_schema_kimball.dim_group_summary s
where b.load_date = (select max(load_date) from iceberg.business_vault.pit_group) and b.hub_group_hash_key = s.hub_group_hash_key;