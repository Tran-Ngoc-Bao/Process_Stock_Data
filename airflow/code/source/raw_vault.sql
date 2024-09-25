create schema if not exists iceberg.raw_vault;
use iceberg.raw_vault;

-- summary
create table if not exists hub_summary
with (format = 'parquet')
as select 
sha1(cast(indexId as varbinary)) as hub_summary_hash_key, 
ETL_time as load_date, indexId as record_source
from summary;

create table if not exists sat_summary
with (format = 'parquet')
as select
sha1(cast(indexId as varbinary)) as hub_summary_hash_key,
sha1(cast(concat(cast(indexValue as varchar), cast(prevIndexValue as varchar), cast(allQty as varchar), cast(allValue as varchar)) as varbinary)) as hash_diff,
ETL_time as load_date, indexId as record_source, *
from summary;

-- group
create table if not exists hub_group
with (format = 'parquet')
as select
sha1(cast(concat(sn, ss, indexId) as varbinary)) as hub_group_hash_key,
ETL_time as load_date, indexId as record_source
from gr;

create table if not exists sat_group_buy
with (format = 'parquet')
as select
sha1(cast(concat(sn, ss, indexId) as varbinary)) as hub_group_hash_key,
sha1(cast(concat(cast(b1 as varchar), cast(b2 as varchar), cast(b3 as varchar)) as varbinary)) as hash_diff,
ETL_time as load_date, indexId as record_source,
b1 as gia1, b1v as kl1, b2 as gia2, b2v as kl2, b3 as gia3, b3v as kl3
from gr;

create table if not exists sat_group_sell
with (format = 'parquet')
as select
sha1(cast(concat(sn, ss, indexId) as varbinary)) as hub_group_hash_key,
sha1(cast(concat(cast(o1 as varchar), cast(o2 as varchar), cast(o3 as varchar)) as varbinary)) as hash_diff,
ETL_time as load_date, indexId as record_source,
o1 as gia1, o1v as kl1, o2 as gia2, o2v as kl2, o3 as gia3, o3v as kl3
from gr;

create table if not exists sat_group_khop_lenh
with (format = 'parquet')
as select
sha1(cast(concat(sn, ss, indexId) as varbinary)) as hub_group_hash_key,
sha1(cast(concat(cast(mp as varchar), cast(cp as varchar), cast(mv as varchar), cast(pc as varchar)) as varbinary)) as hash_diff,
ETL_time as load_date, indexId as record_source,
mp as gia, cp as ctpercent, mv as kl, pc as ct
from gr;

create table if not exists sat_group_dtnn
with (format = 'parquet')
as select
sha1(cast(concat(sn, ss, indexId) as varbinary)) as hub_group_hash_key,
sha1(cast(concat(cast(bfq as varchar), cast(rfq as varchar), cast(sfq as varchar)) as varbinary)) as hash_diff,
ETL_time as load_date, indexId as record_source,
bfq as nnmua, rfq as room, sfq as nnban
from gr;

create table if not exists sat_group_summary
with (format = 'parquet')
as select
sha1(cast(concat(sn, ss, indexId) as varbinary)) as hub_group_hash_key,
sha1(cast(concat(ss, ce) as varbinary)) as hash_diff,
ETL_time as load_date, indexId as record_source,
ss as ck, sen, ce, cv, c as trann, f as san, pcp as tc, mtq as tongkl, h as cao, l as thap
from gr;

create table if not exists sat_group_other
with (format = 'parquet')
as select
sha1(cast(concat(sn, ss, indexId) as varbinary)) as hub_group_hash_key,
sha1(cast(concat(sn, cast(ap as varchar), cast(l as varchar)) as varbinary)) as hash_diff,
ETL_time as load_date, indexId as record_source,
sn, ap, cpe, cs, cwt, e, ep, er, isn, ltd, lv, md, mtv, o, r, st, ts, tsh, tu, us, os, s, lmp, lmv, lpc, lpcp, mtq, ptq, ptv
from gr;

-- exchange_index
create table if not exists hub_exchange_index
with (format = 'parquet')
as select
sha1(cast(indexId as varbinary)) as hub_exchange_index_hash_key,
ETL_time as load_date, indexId as record_source
from exchange_index;

create table if not exists sat_exchange_index
with (format = 'parquet')
as select
sha1(cast(indexId as varbinary)) as hub_exchange_index_hash_key,
sha1(cast(concat(cast(indexValue as varchar), cast(vol as varchar), cast(totalQtty as varchar)) as varbinary)) as hash_diff,
ETL_time as load_date, indexId as record_source, *
from exchange_index;

-- link
create table if not exists link_summary_ei_group
with (format = 'parquet')
as select
sha1(concat(hub_summary_hash_key, hub_group_hash_key)) as link_summary_ei_hash_key,
hub_summary_hash_key, hub_group_hash_key, s.load_date, s.record_source
from hub_summary as s, hub_group as g
where s.load_date = g.load_date and s.record_source = g.record_source;

-- summary
insert into hub_summary
select 
sha1(cast(indexId as varbinary)) as hub_summary_hash_key, 
ETL_time as load_date, indexId as record_source
from summary
where ETL_time = (select max(ETL_time) from summary);

insert into sat_summary
select
sha1(cast(indexId as varbinary)) as hub_summary_hash_key,
sha1(cast(concat(cast(indexValue as varchar), cast(prevIndexValue as varchar), cast(allQty as varchar), cast(allValue as varchar)) as varbinary)) as hash_diff,
ETL_time as load_date, indexId as record_source, *
from summary
where ETL_time = (select max(ETL_time) from summary);

-- group
insert into hub_group
select
sha1(cast(concat(sn, ss, indexId) as varbinary)) as hub_group_hash_key,
ETL_time as load_date, indexId as record_source
from gr
where ETL_time = (select max(ETL_time) from gr);

insert into sat_group_buy
select
sha1(cast(concat(sn, ss, indexId) as varbinary)) as hub_group_hash_key,
sha1(cast(concat(cast(b1 as varchar), cast(b2 as varchar), cast(b3 as varchar)) as varbinary)) as hash_diff,
ETL_time as load_date, indexId as record_source,
b1 as gia1, b1v as kl1, b2 as gia2, b2v as kl2, b3 as gia3, b3v as kl3
from gr
where ETL_time = (select max(ETL_time) from gr);

insert into sat_group_sell
select
sha1(cast(concat(sn, ss, indexId) as varbinary)) as hub_group_hash_key,
sha1(cast(concat(cast(o1 as varchar), cast(o2 as varchar), cast(o3 as varchar)) as varbinary)) as hash_diff,
ETL_time as load_date, indexId as record_source,
o1 as gia1, o1v as kl1, o2 as gia2, o2v as kl2, o3 as gia3, o3v as kl3
from gr
where ETL_time = (select max(ETL_time) from gr);

insert into sat_group_khop_lenh
select
sha1(cast(concat(sn, ss, indexId) as varbinary)) as hub_group_hash_key,
sha1(cast(concat(cast(mp as varchar), cast(cp as varchar), cast(mv as varchar), cast(pc as varchar)) as varbinary)) as hash_diff,
ETL_time as load_date, indexId as record_source,
mp as gia, cp as ctpercent, mv as kl, pc as ct
from gr
where ETL_time = (select max(ETL_time) from gr);

insert into sat_group_dtnn
select
sha1(cast(concat(sn, ss, indexId) as varbinary)) as hub_group_hash_key,
sha1(cast(concat(cast(bfq as varchar), cast(rfq as varchar), cast(sfq as varchar)) as varbinary)) as hash_diff,
ETL_time as load_date, indexId as record_source,
bfq as nnmua, rfq as room, sfq as nnban
from gr
where ETL_time = (select max(ETL_time) from gr);

insert into sat_group_summary
select
sha1(cast(concat(sn, ss, indexId) as varbinary)) as hub_group_hash_key,
sha1(cast(concat(ss, ce) as varbinary)) as hash_diff,
ETL_time as load_date, indexId as record_source,
ss as ck, sen, ce, cv, c as trann, f as san, pcp as tc, mtq as tongkl, h as cao, l as thap
from gr
where ETL_time = (select max(ETL_time) from gr);

insert into sat_group_other
select
sha1(cast(concat(sn, ss, indexId) as varbinary)) as hub_group_hash_key,
sha1(cast(concat(sn, cast(ap as varchar), cast(l as varchar)) as varbinary)) as hash_diff,
ETL_time as load_date, indexId as record_source,
sn, ap, cpe, cs, cwt, e, ep, er, isn, ltd, lv, md, mtv, o, r, st, ts, tsh, tu, us, os, s, lmp, lmv, lpc, lpcp, mtq, ptq, ptv
from gr
where ETL_time = (select max(ETL_time) from gr);

-- exchange_index
insert into hub_exchange_index
select
sha1(cast(indexId as varbinary)) as hub_exchange_index_hash_key,
ETL_time as load_date, indexId as record_source
from exchange_index
where ETL_time = (select max(ETL_time) from exchange_index);

insert into sat_exchange_index
select
sha1(cast(indexId as varbinary)) as hub_exchange_index_hash_key,
sha1(cast(concat(cast(indexValue as varchar), cast(vol as varchar), cast(totalQtty as varchar)) as varbinary)) as hash_diff,
ETL_time as load_date, indexId as record_source, *
from exchange_index
where ETL_time = (select max(ETL_time) from exchange_index);

-- link
insert into link_summary_ei_group
select
sha1(concat(hub_summary_hash_key, hub_group_hash_key)) as link_summary_ei_hash_key,
hub_summary_hash_key, hub_group_hash_key, s.load_date, s.record_source
from hub_summary as s, hub_group as g
where s.load_date = g.load_date and s.record_source = g.record_source
and s.load_date = (select max(load_date) from hub_summary);
