create or replace view public.vw_semaforo_silver_ready
as
select tab1.uid,tab1.logical_table, tab1.source_id,ttc.real_table  as destination_id,tab1.tipo_caricamento, tab1.key,tab1.query_param, false as is_heavy
from
    (select uid,run_id,tss.key, tss.query_param,tss.logical_table,ttc.real_table as source_id,tss.tipo_caricamento as tipo_caricamento
        from public.tab_semaforo_steps tss join public.tab_table_configs ttc on tss.logical_table =ttc.logical_table
        and ttc.layer=tss.layer
    )tab1 join
tab_table_configs ttc
on tab1.logical_table=ttc.logical_table
where ttc.layer='silver';
