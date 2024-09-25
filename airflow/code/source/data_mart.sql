use iceberg.data_mart;

insert into history
select *
from iceberg.star_schema_kimball.dim_exchange_index
where load_date = (select max(load_date) from iceberg.business_vault.pit_exchange_index);

insert into history_summary
select *
from iceberg.star_schema_kimball.dim_summary
where load_date = (select max(load_date) from iceberg.business_vault.pit_summary);

insert into buy
select b.hub_group_hash_key, b.hash_diff buy_hash_diff, b.load_date, b.record_source, s.hash_diff summary_hash_diff,
gia1, kl1, gia2, kl2, gia3, kl3,
ck, sen, ce, cv, trann, san, tc, tongkl, cao, thap
from iceberg.star_schema_kimball.dim_group_buy b, iceberg.star_schema_kimball.dim_group_summary s
where b.load_date = (select max(load_date) from iceberg.business_vault.pit_group) and b.load_date = s.load_date and b.hub_group_hash_key = s.hub_group_hash_key;

insert into sell
select b.hub_group_hash_key, b.hash_diff buy_hash_diff, b.load_date, b.record_source, s.hash_diff summary_hash_diff,
gia1, kl1, gia2, kl2, gia3, kl3,
ck, sen, ce, cv, trann, san, tc, tongkl, cao, thap
from iceberg.star_schema_kimball.dim_group_sell b, iceberg.star_schema_kimball.dim_group_summary s
where b.load_date = (select max(load_date) from iceberg.business_vault.pit_group) and b.load_date = s.load_date and b.hub_group_hash_key = s.hub_group_hash_key;

insert into khop_lenh
select b.hub_group_hash_key, b.hash_diff buy_hash_diff, b.load_date, b.record_source, s.hash_diff summary_hash_diff,
gia, ctpercent, kl, ct,
ck, sen, ce, cv, trann, san, tc, tongkl, cao, thap
from iceberg.star_schema_kimball.dim_group_khop_lenh b, iceberg.star_schema_kimball.dim_group_summary s
where b.load_date = (select max(load_date) from iceberg.business_vault.pit_group) and b.load_date = s.load_date and b.hub_group_hash_key = s.hub_group_hash_key;

insert into dtnn
select b.hub_group_hash_key, b.hash_diff buy_hash_diff, b.load_date, b.record_source, s.hash_diff summary_hash_diff,
nnmua, room, nnban,
ck, sen, ce, cv, trann, san, tc, tongkl, cao, thap
from iceberg.star_schema_kimball.dim_group_dtnn b, iceberg.star_schema_kimball.dim_group_summary s
where b.load_date = (select max(load_date) from iceberg.business_vault.pit_group) and b.load_date = s.load_date and b.hub_group_hash_key = s.hub_group_hash_key;