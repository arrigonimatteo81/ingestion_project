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
                    WHEN s.tipo_caricamento::text = ANY (ARRAY['TABUT'::text, 'Tabut'::text, 'Estraz'::text, 'ESTRAZ'::text, 'DOMINI'::text, 'Domini'::text]) THEN jsonb_build_object('id', s.id)
                    ELSE jsonb_build_object('id', s.id, 'cod_abi', s.cod_abi, 'cod_provenienza', s.provenienza, 'num_periodo_rif', s.periodo_rif, 'cod_colonna_valore', s.colonna_valore, 'num_ambito',
                    s.ambito, 'id_file', s.id_file , 'max_data_va',
                    CASE
                        WHEN s.tipo_caricamento::text = ANY (ARRAY['CLIENTI'::text, 'Clienti'::text]) THEN COALESCE(r_spec.max_data_va, 20000101::bigint)
                        ELSE COALESCE(r_spec.max_data_va, '20000101000000000'::bigint)
                    END)
                END AS query_param
           FROM ( SELECT s1.id,
                    s1.tabella,
                    ttc.real_table,
                    s1.tipo_caricamento,
                    s1.colonna_valore,
                    s1.ambito,
                    s1.cod_abi,
                    s1.provenienza,
                    s1.periodo_rif,
                    s1.id_file,
                        CASE
                            WHEN s1.tipo_caricamento::text = ANY (ARRAY['TABUT'::text, 'Tabut'::text, 'Estraz'::text, 'ESTRAZ'::text]) THEN jsonb_build_object('cod_tabella', s1.tabella)
                            ELSE jsonb_build_object('cod_abi', s1.cod_abi, 'cod_tabella', s1.tabella, 'cod_provenienza', s1.provenienza)
                        END AS key
                   FROM public.tab_semaforo_mensile s1
                     LEFT JOIN public.tab_table_configs ttc ON s1.tabella::text = ttc.logical_table::text
                  WHERE ttc.layer::text = 'stage'::text AND s1.id_file::text = ''::text) s
             LEFT JOIN public.tab_registro_mensile r_spec ON r_spec.chiave = s.key
          WHERE r_spec.last_id IS NULL OR s.id > r_spec.last_id
        UNION ALL
         SELECT gen_random_uuid() AS gen_random_uuid,
            s.tabella,
            s.tabella,
            s.real_table,
            s.tipo_caricamento,
            s.key,
            jsonb_build_object('id', s.id) AS jsonb_build_object
           FROM ( SELECT s1.id,
                    s1.tabella,
                    ttc.real_table,
                    s1.tipo_caricamento,
                    jsonb_build_object('cod_tabella', s1.tabella) AS key
                   FROM public.tab_semaforo_domini s1
                     LEFT JOIN public.tab_table_configs ttc ON s1.tabella::text = ttc.logical_table::text
                  WHERE ttc.layer::text = 'stage'::text) s
             LEFT JOIN public.tab_registro_mensile r ON r.chiave = s.key
          WHERE r.last_id IS NULL OR s.id > r.last_id
        UNION ALL
         SELECT gen_random_uuid() AS gen_random_uuid,
            s.tabella,
            s.tabella,
            ttc.real_table,
            s.tipo_caricamento,
            jsonb_build_object('cod_tabella', s.tabella) AS jsonb_build_object,
            '{}'::jsonb AS jsonb
           FROM public.tab_no_semaforo s
             LEFT JOIN public.tab_table_configs ttc ON s.tabella::text = ttc.logical_table::text
          WHERE ttc.layer::text = 'stage'::text

     union all

     select
gen_random_uuid() AS uid,
	tabella as logical_table,
	tabella as source_id,
    real_table as destination_id,
    tipo_caricamento,
    key,
    jsonb_build_object(
                'cod_abi', cod_abi,
                'cod_provenienza', provenienza,
                'num_periodo_confronto',periodo_di_confronto,
                'cod_colonna_valore', colonna_valore,
                'num_ambito', ambito,
				'files', array_agg(DISTINCT id_file ORDER BY id_file)
            )
        AS query_param
FROM
	(select s.*, COALESCE(r_base.periodo, to_char(current_date - interval '1 month', 'YYYYMM')::int) as periodo_di_confronto
	FROM (
        SELECT
            s1.id,
            s1.tabella,
            ttc.real_table,
            s1.tipo_caricamento,
            s1.colonna_valore,
            s1.ambito,
            s1.cod_abi,
            s1.provenienza,
            s1.periodo_rif,
			s1.id_file,
            jsonb_build_object(
                    'cod_abi', s1.cod_abi,
                    'cod_tabella', s1.tabella,
                    'cod_provenienza', s1.provenienza
                )
            AS key
        FROM public.tab_semaforo_mensile s1
        LEFT JOIN public.tab_table_configs ttc
            ON s1.tabella::text = ttc.logical_table::text
        WHERE ttc.layer::text = 'stage' and s1.id_file !=''
    ) s
    LEFT JOIN public.tab_registro_mensile r_base
        ON s.tabella LIKE '%\_RETT'
       AND (r_base.chiave ->> 'cod_abi') = (s.key ->> 'cod_abi')
       AND regexp_replace(r_base.chiave ->> 'cod_tabella', '_.*$', '') =
           regexp_replace(s.key ->> 'cod_tabella', '_.*$', '')
       AND (r_base.chiave ->> 'cod_provenienza') =
           (s.key ->> 'cod_provenienza')
    LEFT JOIN public.tab_registro_rettifiche r_rett
      ON r_rett.chiave = s.key and r_rett.id_file =s.id_file
      where r_rett.chiave is null)tb1
      GROUP BY key,cod_abi,colonna_valore,
                provenienza,
                periodo_di_confronto,
                ambito, tabella,source_id, tipo_caricamento, destination_id
          ) tb1
     LEFT JOIN public.tab_task_configs ON tb1.key = tab_task_configs.key;