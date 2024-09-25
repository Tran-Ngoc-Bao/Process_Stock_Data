use iceberg.star_schema_kimball;

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