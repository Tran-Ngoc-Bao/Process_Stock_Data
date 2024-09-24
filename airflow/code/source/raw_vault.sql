use iceberg.raw_vault;

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
sha1(cast(concat(sn, e, ss, indexId, ETL_time, td) as varbinary)) as hub_group_hash_key,
ETL_time as load_date, indexId as record_source
from gr
where ETL_time = (select max(ETL_time) from gr);

insert into sat_group
select
sha1(cast(concat(sn, e, ss, indexId, ETL_time, td) as varbinary)) as hub_group_hash_key,
sha1(cast(concat(st, s, ce, cv) as varbinary)) as hash_diff,
ETL_time as load_date, indexId as record_source, *
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

-- odd_exchange
insert into hub_odd_exchange
select
sha1(cast(concat(sn, e, ss, ETL_time, cast(ltu as varchar), td) as varbinary)) as hub_odd_exchange_hash_key,
ETL_time as load_date, e as record_source
from odd_exchange
where ETL_time = (select max(ETL_time) from odd_exchange);

insert into sat_odd_exchange
select
sha1(cast(concat(sn, e, ss, ETL_time, cast(ltu as varchar), td) as varbinary)) as hub_odd_exchange_hash_key,
sha1(cast(concat(st, s, ce, cv) as varbinary)) as hash_diff,
ETL_time as load_date, e as record_source, *
from odd_exchange
where ETL_time = (select max(ETL_time) from odd_exchange);

-- put_exec
insert into hub_put_exec
select
sha1(cast(concat(_id, stockSymbol, exchange, createdAt, ETL_time) as varbinary)) as hub_put_exec_hash_key,
ETL_time as load_date, exchange as record_source
from put_exec
where ETL_time = (select max(ETL_time) from put_exec);

insert into sat_put_exec
select
sha1(cast(concat(_id, stockSymbol, exchange, createdAt, ETL_time) as varbinary)) as hub_put_exec_hash_key,
sha1(cast(concat(cast(val as varchar), cast(price as varchar), cast(ptTotalTradedQty as varchar), cast(ptTotalTradedValue as varchar)) as varbinary)) as hash_diff,
ETL_time as load_date, exchange as record_source, *
from put_exec
where ETL_time = (select max(ETL_time) from put_exec);

-- link
insert into link_summary_ei
select
sha1(concat(hub_summary_hash_key, hub_exchange_index_hash_key)) as link_summary_ei_hash_key,
hub_summary_hash_key, hub_exchange_index_hash_key, s.load_date, s.record_source
from hub_summary as s, hub_exchange_index as ei
where s.load_date = (select max(load_date) from hub_summary)
and s.load_date = ei.load_date and s.record_source = ei.record_source;

insert into link_summary_group
select
sha1(concat(hub_summary_hash_key, hub_group_hash_key)) as link_summary_group_hash_key,
hub_summary_hash_key, hub_group_hash_key, s.load_date, s.record_source
from hub_summary as s, hub_group as g
where s.load_date = (select max(load_date) from hub_summary)
and s.load_date = g.load_date and s.record_source = g.record_source;

insert into link_oe_pe
select
sha1(concat(hub_odd_exchange_hash_key, hub_put_exec_hash_key)) as link_oe_pe_hash_key,
hub_odd_exchange_hash_key, hub_put_exec_hash_key, oe.load_date, oe.record_source
from hub_odd_exchange as oe, hub_put_exec as pe
where oe.load_date = (select max(load_date) from hub_odd_exchange)
and oe.load_date = pe.load_date and oe.record_source = pe.record_source;
