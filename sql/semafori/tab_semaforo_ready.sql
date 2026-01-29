CREATE OR REPLACE VIEW public.vw_semaforo_stage_ready
AS SELECT tb1.uid,
    tb1.logical_table,
    tb1.source_id,
    tb1.destination_id,
    tb1.tipo_caricamento,
    tb1.key,
    tb1.query_param,
    COALESCE(tab_task_configs.is_heavy, false) AS is_heavy
   FROM ( SELECT gen_random_uuid() AS uid,
            s.tabella AS logical_table,
            s.tabella AS source_id,
            s.real_table AS destination_id,
            s.tipo_caricamento,
            s.key,
                CASE
                    WHEN s.tipo_caricamento::text = ANY (ARRAY['TABUT'::character varying::text, 'Tabut'::character varying::text, 'Estraz'::character varying::text, 'ESTRAZ'::character varying::text, 'DOMINI'::character varying::text, 'Domini'::character varying::text]) THEN jsonb_build_object('id', s.id)
                    ELSE jsonb_build_object('id', s.id, 'cod_abi', s.cod_abi, 'cod_provenienza', s.provenienza, 'num_periodo_rif', s.periodo_rif, 'cod_colonna_valore', s.colonna_valore, 'num_ambito', s.ambito, 'max_data_va',
                    CASE
                        WHEN s.tipo_caricamento::text = ANY (ARRAY['CLIENTI'::character varying::text, 'Clienti'::character varying::text]) THEN COALESCE(r.max_data_va, 20000101::bigint)
                        ELSE COALESCE(r.max_data_va, '20000101000000000'::bigint)
                    END)
                END AS query_param
           FROM ( SELECT s_1.id,
                    s_1.tabella,
                    ttc.real_table,
                    s_1.tipo_caricamento,
                    s_1.colonna_valore,
                    s_1.ambito,
                    s_1.cod_abi,
                    s_1.provenienza,
                    s_1.periodo_rif,
                        CASE
                            WHEN s_1.tipo_caricamento::text = ANY (ARRAY['TABUT'::character varying::text, 'Tabut'::character varying::text, 'Estraz'::character varying::text, 'ESTRAZ'::character varying::text]) THEN jsonb_build_object('cod_tabella', s_1.tabella)
                            ELSE jsonb_build_object('cod_abi', s_1.cod_abi, 'cod_tabella', s_1.tabella, 'cod_provenienza', s_1.provenienza)
                        END AS key
                   FROM public.tab_semaforo_mensile s_1
                     LEFT JOIN public.tab_table_configs ttc ON s_1.tabella::text = ttc.logical_table::text
                  WHERE ttc.layer::text = 'stage'::text) s
             LEFT JOIN public.tab_registro_mensile r ON r.chiave = s.key
          WHERE r.last_id IS NULL OR s.id > r.last_id
        UNION ALL
         SELECT gen_random_uuid() AS uid,
            s.tabella AS logical_table,
            s.tabella AS source_id,
            s.real_table AS destination_id,
            s.tipo_caricamento,
            s.key,
            jsonb_build_object('id', s.id) AS query_param
           FROM ( SELECT s_1.id,
                    s_1.tabella,
                    ttc.real_table,
                    s_1.tipo_caricamento,
                    s_1.colonna_valore,
                    s_1.ambito,
                    s_1.cod_abi,
                    s_1.provenienza,
                    s_1.periodo_rif,
                    jsonb_build_object('cod_tabella', s_1.tabella) AS key
                   FROM public.tab_semaforo_domini s_1
                     LEFT JOIN public.tab_table_configs ttc ON s_1.tabella::text = ttc.logical_table::text
                  WHERE ttc.layer::text = 'stage'::text) s
             LEFT JOIN public.tab_registro_mensile r ON r.chiave = s.key
          WHERE r.last_id IS NULL OR s.id > r.last_id
        UNION ALL
         SELECT gen_random_uuid() AS uid,
            s.tabella AS logical_table,
            s.tabella AS source_id,
            ttc.real_table AS destination_id,
            s.tipo_caricamento,
             jsonb_build_object('cod_tabella', s_2.tabella) AS key,
             '{}'::jsonb AS query_param
           FROM public.tab_no_semaforo s_2
             LEFT JOIN public.tab_table_configs ttc ON s.tabella::text = ttc.logical_table::text
          WHERE ttc.layer::text = 'stage'::text) tb1
     LEFT JOIN public.tab_task_configs ON tb1.key = tab_task_configs.key