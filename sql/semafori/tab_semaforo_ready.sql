CREATE OR REPLACE VIEW public.vw_semaforo_stage_ready
AS
SELECT
    tb1.uid,
    tb1.logical_table,
    tb1.source_id,
    tb1.destination_id,
    tb1.tipo_caricamento,
    tb1.key,
    tb1.query_param,
    COALESCE(tab_task_configs.is_heavy, false) AS is_heavy
FROM (

    /* =======================
       SEZIONE SEMAFORO MENSILE
       ======================= */
    SELECT
        gen_random_uuid() AS uid,
        s.tabella AS logical_table,
        s.tabella AS source_id,
        s.real_table AS destination_id,
        s.tipo_caricamento,
        s.key,

        CASE
            WHEN s.tipo_caricamento::text = ANY (
                ARRAY['TABUT','Tabut','Estraz','ESTRAZ','DOMINI','Domini']
            )
            THEN jsonb_build_object('id', s.id)

            ELSE jsonb_build_object(
                'id', s.id,
                'cod_abi', s.cod_abi,
                'cod_provenienza', s.provenienza,
                'num_periodo_rif',
                CASE
                    WHEN s.tabella LIKE '%\_RET'
                    	THEN COALESCE(r_base.periodo, to_char(current_date - interval '1 month', 'YYYYMM')::int)
                    ELSE s.periodo_rif
                END,

                'cod_colonna_valore', s.colonna_valore,
                'num_ambito', s.ambito,
				'id_file', s.id_file,
                'max_data_va',
                CASE
                    WHEN s.tabella LIKE '%\_RET'
                    THEN
                        CASE
                            WHEN s.tipo_caricamento::text = ANY (ARRAY['CLIENTI','Clienti'])
                            THEN COALESCE(r_base.max_data_va, 20000101::bigint)
                            ELSE COALESCE(r_base.max_data_va, 20000101000000000::bigint)
                        END
                    ELSE
                        CASE
                            WHEN s.tipo_caricamento::text = ANY (ARRAY['CLIENTI','Clienti'])
                            THEN COALESCE(r_spec.max_data_va, 20000101::bigint)
                            ELSE COALESCE(r_spec.max_data_va, 20000101000000000::bigint)
                        END
                END
            )
        END AS query_param

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
            CASE
                WHEN s1.tipo_caricamento::text = ANY (
                    ARRAY['TABUT','Tabut','Estraz','ESTRAZ']
                )
                THEN jsonb_build_object('cod_tabella', s1.tabella)
                ELSE jsonb_build_object(
                    'cod_abi', s1.cod_abi,
                    'cod_tabella', s1.tabella,
                    'cod_provenienza', s1.provenienza
                )
            END AS key

        FROM public.tab_semaforo_mensile s1
        LEFT JOIN public.tab_table_configs ttc
            ON s1.tabella::text = ttc.logical_table::text
        WHERE ttc.layer::text = 'stage'
    ) s

    /* join sulla chiave SPECIFICA (READDR_RET) */
    LEFT JOIN public.tab_registro_mensile r_spec
        ON r_spec.chiave = s.key

    /* join sulla chiave BASE (READDR) */
    LEFT JOIN public.tab_registro_mensile r_base
        ON s.tabella LIKE '%\_RET'
       AND (r_base.chiave ->> 'cod_abi') = (s.key ->> 'cod_abi')
       AND regexp_replace(r_base.chiave ->> 'cod_tabella', '_.*$', '') =
           regexp_replace(s.key ->> 'cod_tabella', '_.*$', '')
       AND (r_base.chiave ->> 'cod_provenienza') =
           (s.key ->> 'cod_provenienza')

    WHERE r_spec.last_id IS NULL OR s.id > r_spec.last_id

    UNION ALL
    /* ===== resto della view IDENTICO ===== */

    SELECT
        gen_random_uuid(),
        s.tabella,
        s.tabella,
        s.real_table,
        s.tipo_caricamento,
        s.key,
        jsonb_build_object('id', s.id)
    FROM (
        SELECT
            s1.id,
            s1.tabella,
            ttc.real_table,
            s1.tipo_caricamento,
            jsonb_build_object('cod_tabella', s1.tabella) AS key
        FROM public.tab_semaforo_domini s1
        LEFT JOIN public.tab_table_configs ttc
            ON s1.tabella::text = ttc.logical_table::text
        WHERE ttc.layer::text = 'stage'
    ) s
    LEFT JOIN public.tab_registro_mensile r
        ON r.chiave = s.key
    WHERE r.last_id IS NULL OR s.id > r.last_id

    UNION ALL

    SELECT
        gen_random_uuid(),
        s.tabella,
        s.tabella,
        ttc.real_table,
        s.tipo_caricamento,
        jsonb_build_object('cod_tabella', s.tabella),
        '{}'::jsonb
    FROM public.tab_no_semaforo s
    LEFT JOIN public.tab_table_configs ttc
        ON s.tabella::text = ttc.logical_table::text
    WHERE ttc.layer::text = 'stage'

) tb1
LEFT JOIN public.tab_task_configs
    ON tb1.key = tab_task_configs.key;
