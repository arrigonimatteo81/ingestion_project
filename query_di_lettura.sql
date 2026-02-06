--query di lettura della READVC con i parametri di file, ambito e provenienza

WITH ex6 AS (
    SELECT DISTINCT
        BANCA,
        CONVERT(CHAR(5), COD_UO) AS COD_UO,
        CONVERT(CHAR(24), NUM_PARTITA) AS NUM_PARTITA,
        PERIODO_RIF,
        COALESCE(periodo_elab, periodo_rif) AS periodo_comp,
        provn,
        idfile,
        TIPO_RETTIFICA
    FROM VEX6_RETT WITH (NOLOCK)
    WHERE idfile = '${file_id}'
      AND provn = '${cod_provenienza}'
      AND TIPO_RETTIFICA IN ('RAP', 'RAPSOG')
)
-- caso 1: mese precedente
SELECT
    readvc.BANCA AS NUM_BANCA,
    LTRIM(RTRIM(readvc.COD_UO)) AS COD_UO,
    LTRIM(RTRIM(readvc.NUM_PARTITA)) AS COD_NUM_PARTITA,
    LTRIM(RTRIM(readvc.COD_PRODOTTO)) AS COD_PRODOTTO,
    LTRIM(RTRIM(readvc.TIPO_OPERAZIONE)) AS COD_TIPO_OPERAZIONE,
    LTRIM(RTRIM(readvc.CANALE)) AS COD_CANALE,
    LTRIM(RTRIM(readvc.PRODOTTO_COMM)) AS COD_PRODOTTO_COMM,
    LTRIM(RTRIM(readvc.PORTAFOGLIO)) AS COD_PORTAFOGLIO,
    LTRIM(RTRIM(readvc.DESK_RENDICONTATIVO)) AS COD_DESK_RENDICONTATIVO,
    LTRIM(RTRIM(readvc.PTF_SPECCHIO)) AS COD_PTF_SPECCHIO,
    LTRIM(RTRIM(readvc.COD_DATO)) AS cod_dato,
    LTRIM(RTRIM(readvc.COD_PROVN_DATI_RED)) AS COD_PROVN_DATI_RED,
    CONVERT(INT, ex6.PERIODO_RIF) AS NUM_PERIODO_RIF,
    CONVERT(INT, ex6.PERIODO_COMP) AS NUM_PERIODO_COMP,
    ISNULL(readvc.${colonna_valore},0) AS imp_valore
FROM ex6
INNER JOIN READVC readvc WITH (NOLOCK, INDEX(PK_READVC))
    ON readvc.BANCA = ex6.BANCA
   AND readvc.COD_UO = ex6.COD_UO
   AND readvc.NUM_PARTITA = ex6.NUM_PARTITA
   AND readvc.COD_PROVN_DATI_RED = ex6.provn
WHERE ex6.idfile = '${file_id}'
  AND readvc.AMBITO = ${cod_ambito}
  AND ex6.PERIODO_RIF <> ex6.PERIODO_COMP
  AND readvc.TIPO_OPERAZIONE LIKE '__' + RIGHT(CONVERT(VARCHAR(6), ex6.PERIODO_COMP),4)

UNION ALL

-- caso 2: mese corrente
SELECT
    readvc.BANCA AS NUM_BANCA,
    LTRIM(RTRIM(readvc.COD_UO)) AS COD_UO,
    LTRIM(RTRIM(readvc.NUM_PARTITA)) AS COD_NUM_PARTITA,
    LTRIM(RTRIM(readvc.COD_PRODOTTO)) AS COD_PRODOTTO,
    LTRIM(RTRIM(readvc.TIPO_OPERAZIONE)) AS COD_TIPO_OPERAZIONE,
    LTRIM(RTRIM(readvc.CANALE)) AS COD_CANALE,
    LTRIM(RTRIM(readvc.PRODOTTO_COMM)) AS COD_PRODOTTO_COMM,
    LTRIM(RTRIM(readvc.PORTAFOGLIO)) AS COD_PORTAFOGLIO,
    LTRIM(RTRIM(readvc.DESK_RENDICONTATIVO)) AS COD_DESK_RENDICONTATIVO,
    LTRIM(RTRIM(readvc.PTF_SPECCHIO)) AS COD_PTF_SPECCHIO,
    LTRIM(RTRIM(readvc.COD_DATO)) AS cod_dato,
    LTRIM(RTRIM(readvc.COD_PROVN_DATI_RED)) AS COD_PROVN_DATI_RED,
    CONVERT(INT, ex6.PERIODO_RIF) AS NUM_PERIODO_RIF,
    CONVERT(INT, ex6.PERIODO_COMP) AS NUM_PERIODO_COMP,
    ISNULL(readvc.${colonna_valore},0) AS imp_valore
FROM ex6
INNER JOIN READVC readvc WITH (NOLOCK, INDEX(PK_READVC))
    ON readvc.BANCA = ex6.BANCA
   AND readvc.COD_UO = ex6.COD_UO
   AND readvc.NUM_PARTITA = ex6.NUM_PARTITA
   AND readvc.COD_PROVN_DATI_RED = ex6.provn
WHERE ex6.idfile = '${file_id}'
  AND readvc.AMBITO = ${cod_ambito}
  AND ex6.PERIODO_RIF = ex6.PERIODO_COMP;

---------------------------------------------------------------------------------------------------

-- lettura del semaforo per TUTTE le rettifiche (cliente, rapporto, dato)
SELECT
    sem.id,
    sem.cod_abi,
	r.periodo_rif,
    CONCAT(sem.TABELLA, '_RETT') AS tabella,
    CONCAT(sem.TIPO_CARICAMENTO, '_rett') as tipo_caricamento,
    sem.PROVENIENZA,
    sem.ID_FILE,
    r.nome_colonna_dvc AS colonna_valore,
    sem.ambito,
    sem.o_carico,
	r.periodo_comp
FROM NPLG0_SEMAFORO_MENSILE sem
JOIN (
    SELECT
        idFile,
        provn,
        MAX(COALESCE(periodo_elab, periodo_rif)) AS periodo_comp,
		MAX(periodo_rif) as periodo_rif,
        MAX(nome_colonna_dvc) AS nome_colonna_dvc
    FROM [RDBP0_MENS].[dbo].[VEX6_RETT]
    GROUP BY idFile, provn
) r
    ON sem.id_file = r.idFile
   AND sem.PROVENIENZA = r.provn
WHERE sem.o_carico >= DATEADD(DAY, -15, CAST(GETDATE() AS DATE)) --girando tutti i giorni si potrebbe andare indietro anche solo di un giorno
  AND
  sem.id_file <> '' and sem.periodo_rif=0
  order by o_carico desc
--------------
-- vista per rettifiche
SELECT
    key,
    colonna_valore,
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
      GROUP BY key, colonna_valore,cod_abi,
                provenienza,
                periodo_di_confronto,
                ambito
ORDER BY key, colonna_valore,cod_abi,
                provenienza,
                periodo_di_confronto,
                ambito;