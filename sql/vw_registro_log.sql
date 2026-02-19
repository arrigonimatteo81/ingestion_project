CREATE OR REPLACE VIEW public.vw_registro_log
AS SELECT trm.num_max_id_riga_semaforo,
    trm.num_max_data_va,
    trm.cod_tipo_caricamento,
    trm.cod_provenienza,
    trm.num_banca,
    trm.cod_nome_tabella,
        CASE
            WHEN trm.cod_nome_tabella::text ~~ '%_RETT'::text THEN '-1'::integer
            WHEN trm.cod_nome_tabella::text ~~ 'TABUT%'::text THEN COALESCE(tlpm.max_periodo_rif, 99999999)
            ELSE COALESCE(tlpm.max_periodo_rif, '-1'::integer)
        END AS max_periodo_rif
   FROM public.tb_registro_mens trm
     LEFT JOIN ( SELECT tb_log_processi_mens.cod_banca,
            tb_log_processi_mens.cod_provenienza,
            tb_log_processi_mens.cod_tipo_caricamento,
            tb_log_processi_mens.cod_tabella,
            max(tb_log_processi_mens.num_periodo_rif) AS max_periodo_rif
           FROM public.tb_log_processi_mens
          WHERE tb_log_processi_mens.cod_step::text = 'gold'::text
          GROUP BY tb_log_processi_mens.cod_banca, tb_log_processi_mens.cod_provenienza, tb_log_processi_mens.cod_tipo_caricamento, tb_log_processi_mens.cod_tabella) tlpm ON lpad(trm.num_banca::text, 5, '0'::text) = tlpm.cod_banca::text AND trm.cod_provenienza::text = tlpm.cod_provenienza::text AND trm.cod_tipo_caricamento::text = tlpm.cod_tipo_caricamento::text AND trm.cod_nome_tabella::text = tlpm.cod_tabella::text;