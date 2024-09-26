
    create schema if not exists iceberg.raw_vault;
    use iceberg.raw_vault;
    call iceberg.system.register_table(schema_name => 'raw_vault', table_name => 'summary_2024092604', table_location => 's3a://warehouse/staging_vault/summary_2024092604');
    call iceberg.system.register_table(schema_name => 'raw_vault', table_name => 'gr_2024092604', table_location => 's3a://warehouse/staging_vault/group_2024092604');
    call iceberg.system.register_table(schema_name => 'raw_vault', table_name => 'exchange_index_2024092604', table_location => 's3a://warehouse/staging_vault/exchange_index_2024092604');
    insert into summary
    select *
    from summary_2024092604;
    insert into gr
    select *
    from gr_2024092604;
    insert into exchange_index
    select *
    from exchange_index_2024092604;
    drop table summary_2024092604;
    drop table exchange_index_2024092604;
    drop table gr_2024092604;
    