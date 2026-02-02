-- public.vw_semaforo_silver_ready source

CREATE OR REPLACE VIEW public.vw_semaforo_silver_ready
AS SELECT tab1.uid,
    tab1.logical_table,
    tab1.source_id,
    ttc.real_table AS destination_id,
    tab1.tipo_caricamento,
    tab1.key,
    tab1.query_param,
    false AS is_heavy
   FROM ( SELECT tss.uid,
            tss.run_id,
            tss.key,
            tss.query_param,
            tss.logical_table,
            ttc_1.real_table AS source_id,
            tss.tipo_caricamento
           FROM public.tab_semaforo_steps tss
             JOIN public.tab_table_configs ttc_1 ON tss.logical_table::text = ttc_1.logical_table::text AND ttc_1.layer::text = tss.layer::text) tab1
     JOIN public.tab_table_configs ttc ON tab1.logical_table::text = ttc.logical_table::text
  WHERE ttc.layer::text = 'silver'::text;