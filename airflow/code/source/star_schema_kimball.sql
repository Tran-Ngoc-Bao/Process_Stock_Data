use iceberg.star_schema_kimball;

-- summary_ei_group
insert into fact_summary_ei_group
select *
from iceberg.business_vault.bridge_summary_ei_group
where load_date = (select max(load_date) from iceberg.business_vault.pit_summary);

insert into dim_summary
select *
from iceberg.raw_vault.sat_summary
where load_date = (select max(load_date) from iceberg.business_vault.pit_summary);

insert into dim_exchange_index
select *
from iceberg.raw_vault.sat_exchange_index
where load_date = (select max(load_date) from iceberg.business_vault.pit_exchange_index);

insert into dim_group
select *
from iceberg.raw_vault.sat_group
where load_date = (select max(load_date) from iceberg.business_vault.pit_group);

-- oe_pe
insert into fact_oe_pe
select *
from iceberg.raw_vault.link_oe_pe
where load_date = (select max(load_date) from iceberg.business_vault.pit_odd_exchange);

insert into dim_odd_exchange
select *
from iceberg.raw_vault.sat_odd_exchange
where load_date = (select max(load_date) from iceberg.business_vault.pit_odd_exchange);

insert into dim_put_exec
select *
from iceberg.raw_vault.sat_put_exec
where load_date = (select max(load_date) from iceberg.business_vault.pit_put_exec);