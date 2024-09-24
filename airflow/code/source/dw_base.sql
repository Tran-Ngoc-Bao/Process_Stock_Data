create schema iceberg.raw_vault;
use iceberg.raw_vault;

-- summary
call iceberg.system.register_table(schema_name => 'raw_vault', table_name => 'summary', table_location => 's3a://warehouse/staging_vault/summary');

create table hub_summary
with (format = 'parquet')
as select 
sha1(cast(indexId as varbinary)) as hub_summary_hash_key, 
ETL_time as load_date, indexId as record_source
from summary;

create table sat_summary
with (format = 'parquet')
as select
sha1(cast(indexId as varbinary)) as hub_summary_hash_key,
sha1(cast(concat(cast(indexValue as varchar), cast(prevIndexValue as varchar), cast(allQty as varchar), cast(allValue as varchar)) as varbinary)) as hash_diff,
ETL_time as load_date, indexId as record_source, *
from summary;

-- group
call iceberg.system.register_table(schema_name => 'raw_vault', table_name => 'gr', table_location => 's3a://warehouse/staging_vault/group');

create table hub_group
with (format = 'parquet')
as select
sha1(cast(concat(sn, ss, indexId, ETL_time) as varbinary)) as hub_group_hash_key,
ETL_time as load_date, indexId as record_source
from gr;

create table sat_group_buy
with (format = 'parquet')
as select
sha1(cast(concat(sn, ss, indexId, ETL_time) as varbinary)) as hub_group_hash_key,
sha1(cast(concat(cast(b1 as varchar), cast(b2 as varchar), cast(b3 as varchar)) as varbinary)) as hash_diff,
ETL_time as load_date, indexId as record_source,
b1 as gia1, b1v as kl1, b2 as gia2, b2v as kl2, b3 as gia3, b3v as kl3
from gr;

create table sat_group_sell
with (format = 'parquet')
as select
sha1(cast(concat(sn, ss, indexId, ETL_time) as varbinary)) as hub_group_hash_key,
sha1(cast(concat(cast(o1 as varchar), cast(o2 as varchar), cast(o3 as varchar)) as varbinary)) as hash_diff,
ETL_time as load_date, indexId as record_source,
o1 as gia1, o1v as kl1, o2 as gia2, o2v as kl2, o3 as gia3, o3v as kl3
from gr;

create table sat_group_khop_lenh
with (format = 'parquet')
as select
sha1(cast(concat(sn, ss, indexId, ETL_time) as varbinary)) as hub_group_hash_key,
sha1(cast(concat(cast(mp as varchar), cast(cp as varchar), cast(mv as varchar), cast(pc as varchar)) as varbinary)) as hash_diff,
ETL_time as load_date, indexId as record_source,
mp as gia, cp as ctpercent, mv as kl, pc as ct
from gr;

create table sat_group_dtnn
with (format = 'parquet')
as select
sha1(cast(concat(sn, ss, indexId, ETL_time) as varbinary)) as hub_group_hash_key,
sha1(cast(concat(cast(bfq as varchar), cast(rfq as varchar), cast(sfq as varchar)) as varbinary)) as hash_diff,
ETL_time as load_date, indexId as record_source,
bfq as nnmua, rfq as room, sfq as nnban
from gr;

create table sat_group_summary
with (format = 'parquet')
as select
sha1(cast(concat(sn, ss, indexId, ETL_time) as varbinary)) as hub_group_hash_key,
sha1(cast(concat(ss, ce) as varbinary)) as hash_diff,
ETL_time as load_date, indexId as record_source,
ss as ck, sen, ce, cv, c as trann, f as san, pcp as tc, mtq as tongkl, h as cao, l as thap
from gr;

create table sat_group_other
with (format = 'parquet')
as select
sha1(cast(concat(sn, ss, indexId, ETL_time) as varbinary)) as hub_group_hash_key,
sha1(cast(concat(sn, cast(ap as varchar), cast(l as varchar)) as varbinary)) as hash_diff,
ETL_time as load_date, indexId as record_source,
sn, ap, cpe, cs, cwt, e, ep, er, isn, ltd, lv, md, mtv, o, r, st, ts, tsh, tu, us, os, s, lmp, lmv, lpc, lpcp, mtq, ptq, ptv
from gr;

-- exchange_index
call iceberg.system.register_table(schema_name => 'raw_vault', table_name => 'exchange_index', table_location => 's3a://warehouse/staging_vault/exchange_index');

create table hub_exchange_index
with (format = 'parquet')
as select
sha1(cast(indexId as varbinary)) as hub_exchange_index_hash_key,
ETL_time as load_date, indexId as record_source
from exchange_index;

create table sat_exchange_index
with (format = 'parquet')
as select
sha1(cast(indexId as varbinary)) as hub_exchange_index_hash_key,
sha1(cast(concat(cast(indexValue as varchar), cast(vol as varchar), cast(totalQtty as varchar)) as varbinary)) as hash_diff,
ETL_time as load_date, indexId as record_source, *
from exchange_index;

-- link
create table link_summary_ei_group
with (format = 'parquet')
as select
sha1(concat(hub_summary_hash_key, hub_exchange_index_hash_key, hub_group_hash_key)) as link_summary_ei_hash_key,
hub_summary_hash_key, hub_exchange_index_hash_key, hub_group_hash_key, s.load_date, s.record_source
from hub_summary as s, hub_exchange_index as ei, hub_group as g
where s.load_date = ei.load_date and s.load_date = g.load_date
and s.record_source = ei.record_source and s.record_source = g.record_source;

create schema iceberg.business_vault;
use iceberg.business_vault;

-- PIT
create table pit_summary
with (format = 'parquet')
as select
hub_summary_hash_key, record_source, load_date, time, timeMaker, ETL_time
from iceberg.raw_vault.sat_summary;

create table pit_group
with (format = 'parquet')
as select
hub_group_hash_key, record_source, load_date
from iceberg.raw_vault.hub_group;

create table pit_exchange_index
with (format = 'parquet')
as select
hub_exchange_index_hash_key, record_source, load_date, time, ETL_time
from iceberg.raw_vault.sat_exchange_index;

-- computed SATs
create table c_sat_group
with (format = 'parquet')
as select
su.hub_group_hash_key, su.load_date, su.record_source,
ck, sen, ce, cv, trann, san, tc, tongkl, cao, thap,
b.gia1 gia1mua, b.kl1 kl1mua, b.gia2 gia2mua, b.kl2 kl2mua, b.gia3 gia3mua, b.kl3 kl3mua,
gia, ctpercent, kl, ct,
s.gia1 gia1ban, s.kl1 kl1ban, s.gia2 gia2ban, s.kl2 kl2ban, s.gia3 gia3ban, s.kl3 kl3ban,
nnmua, room, nnban
from
iceberg.raw_vault.sat_group_summary su,
iceberg.raw_vault.sat_group_buy b,
iceberg.raw_vault.sat_group_sell s,
iceberg.raw_vault.sat_group_khop_lenh kl,
iceberg.raw_vault.sat_group_dtnn dtnn
where
su.hub_group_hash_key = b.hub_group_hash_key
and su.hub_group_hash_key = s.hub_group_hash_key
and su.hub_group_hash_key = kl.hub_group_hash_key
and su.hub_group_hash_key = dtnn.hub_group_hash_key;

create schema iceberg.star_schema_kimball;
use iceberg.star_schema_kimball;

create table fact_summary_ei_group
with (format = 'parquet')
as select *
from iceberg.raw_vault.link_summary_ei_group;

create table dim_summary
with (format = 'parquet')
as select *
from iceberg.raw_vault.sat_summary;

create table dim_exchange_index
with (format = 'parquet')
as select *
from iceberg.raw_vault.sat_exchange_index;

create table dim_group
with (format = 'parquet')
as select *
from iceberg.business_vault.c_sat_group;
