-- bridge
create table link_summary_ei
with (
    format = 'parquet',
    location = 's3a://warehouse/raw_vault/link_summary_ei'
) as
select s.exchange, s.indexId, s.time as summaryTime, timeMaker, label, exchangeLabel, s.ETL_time, ei.time as eiTime
from hub_summary as s, hub_exchange_index as ei
where s.exchange = ei.exchange and s.indexId = ei.indexId and s.ETL_time = ei.ETL_time;