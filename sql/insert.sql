INSERT INTO public.tab_task_configs
("name", description, main_python_file, additional_python_file_uris, jar_file_uris, additional_file_uris, archive_file_uris, logging_config, dataproc_properties, processor_type)
VALUES('03239-REAGDG-AN', 'configurazione per REAGDG banca 3239', 'main_processor.py', '{}', '{jar/mssql-jdbc-12.2.0.jre11.jar}', '{}', '{}', NULL, NULL, 'spark');
INSERT INTO public.tab_task_configs
("name", description, main_python_file, additional_python_file_uris, jar_file_uris, additional_file_uris, archive_file_uris, logging_config, dataproc_properties, processor_type)
VALUES('DEFAULT', 'configurazione di default', 'main_processor.py', '{}', '{jar/mssql-jdbc-12.2.0.jre11.jar}', '{}', '{}', NULL, NULL, 'spark');
INSERT INTO public.tab_task_configs
("name", description, main_python_file, additional_python_file_uris, jar_file_uris, additional_file_uris, archive_file_uris, logging_config, dataproc_properties, processor_type)
VALUES('TABUT_BIL_DIVISE', 'configurazione per tabella TABUT_BIL_DIVISE', 'main_processor.py', '{}', '{}', '{}', '{}', NULL, NULL, 'native');
INSERT INTO public.tab_task_configs
("name", description, main_python_file, additional_python_file_uris, jar_file_uris, additional_file_uris, archive_file_uris, logging_config, dataproc_properties, processor_type)
VALUES('01025-READVC-M3', 'configurazione per READVC banca 1025 provenienza K3', 'main_processor.py', '{}', '{jar/mssql-jdbc-12.2.0.jre11.jar}', '{}', '{}', NULL, NULL, 'spark');
-----------------------------------------------------------------------------------------------------------------
INSERT INTO public.tab_tasks
(id, source_id, destination_id, description, config_profile, is_blocking)
VALUES('task_test_1', 'bqcommands_source', 'my_destination', 'ingestion tabella bq_commands', 'config1', false);


INSERT INTO public.tab_task_group (task_id,group_name) VALUES
	 ('task_test_1','TEST_GROUP');

INSERT INTO public.tab_task_destinations
(id, destination_id, destination_type)
VALUES('id1', 'my_destination', 'file');

INSERT INTO public.tab_file_destinations
(destination_id, format_file, gcs_path, csv_separator)
VALUES('my_destination', 'csv', 'data_output/my_destination',',');

INSERT INTO public.tab_task_sources
(id, source_id, source_type)
VALUES('id1', 'bqcommands_source', 'jdbc');

INSERT INTO public.tab_jdbc_sources
(source_id, url, username, pwd, driver, tablename, query_text, partitioning_expression, num_partitions)
VALUES('bqcommands_source', 'jdbc:postgresql://vmgcldlsv1257.syssede.systest.sanpaoloimi.com:5506/tfdir0',
 'fdir_app', 'fdir_app', 'org.postgresql.Driver', 'bqcommands_source', NULL, NULL, NULL);
---------------------------------------------------------------------------------------------------------------
INSERT INTO public.tab_tasks
(id, source_id, destination_id, description, config_profile, is_blocking)
VALUES('task_test_2', 'parquet_source', 'csv_destination', 'elaborazione Parquet', 'config1', false);

INSERT INTO public.tab_task_group (task_id,group_name) VALUES
	 ('task_test_2','TEST_GROUP_2');

INSERT INTO public.tab_task_destinations
(id, destination_id, destination_type)
VALUES('id2', 'csv_destination', 'file');

INSERT INTO public.tab_file_destinations
(destination_id, format_file, gcs_path, overwrite, csv_separator)
VALUES('csv_destination', 'csv', 'data_output/my_new_destination', False, ',');

INSERT INTO public.tab_task_sources
(id, source_id, source_type)
VALUES('id2', 'parquet_source', 'file');

INSERT INTO public.tab_file_sources
(source_id, file_type, "path")
VALUES('parquet_source', 'parquet', 'data_output/my_destination');

---------------------------------------------------------------------------------------------------------------
INSERT INTO public.tab_tasks
(id, source_id, destination_id, description, config_profile, is_blocking)
VALUES('task_test_3', 'csv_source', 'parquet_destination', 'elaborazione csv vs Parquet', 'config1', false);


INSERT INTO public.tab_task_group (task_id,group_name) VALUES ('task_test_3','TEST_GROUP_3');

INSERT INTO public.tab_task_destinations
(id, destination_id, destination_type)
VALUES('id3', 'parquet_destination', 'file');

INSERT INTO public.tab_file_destinations
(destination_id, format_file, gcs_path, overwrite)
VALUES('parquet_destination', 'parquet', 'data_output/my_destination', False);

INSERT INTO public.tab_task_sources
(id, source_id, source_type)
VALUES('id3', 'csv_source', 'file');

INSERT INTO public.tab_file_sources
(source_id, file_type, "path", "csv_separator")
VALUES('csv_source', 'csv', 'data_output/my_new_destination', ';');

--------------------ingestion semaforo-------------------------------------------------------------------------------------------
INSERT INTO public.tab_tasks(da correggere la tab_tasks non esiste più)
(id, source_id, destination_id, description, config_profile, is_blocking)
VALUES('task_semaforo', 'semaforo_source', 'semaforo_destination', 'elaborazione semaforo', 'config1', false);

INSERT INTO public.tab_task_group (task_id,group_name) VALUES ('task_semaforo','GROUP_SEMAFORO');

INSERT INTO public.tab_task_destinations
(id, destination_id, destination_type)
VALUES('id4', 'semaforo_destination', 'jdbc');

INSERT INTO public.tab_jdbc_destinations
(destination_id, url, username, pwd, driver, tablename, overwrite)
VALUES('semaforo_destination', 'jdbc:postgresql://vmgcldlsv1257.syssede.systest.sanpaoloimi.com:5506/tfdir0', 'fdir_app', 'fdir_app', 'org.postgresql.Driver', 'public.SEMAFORO_MENSILE', true);

INSERT INTO public.tab_task_sources
(id, source_id, source_type)
VALUES('id4', 'semaforo_source', 'jdbc');

INSERT INTO public.tab_jdbc_sources
(source_id, url, username, pwd, driver, tablename, query_text, partitioning_expression, num_partitions)
VALUES('semaforo_source', 'jdbc:sqlserver://pdbclt076.syssede.systest.sanpaoloimi.com\\\\SYD202:1433;DatabaseName=RDBP0_MENS;encrypt=true;trustServerCertificate=true;integratedSecurity=true;authenticationScheme=NTLM;', 'SYS_LG_RDB@SYSSPIMI', '4EfTw@B9UpCriK#epGiM', 'com.microsoft.sqlserver.jdbc.SQLServerDriver', NULL,
'select max(id) as ID,COD_ABI,max(PERIODO_RIF) as PERIODO_RIF,TABELLA,TIPO_CARICAMENTO,PROVENIENZA,ID_FILE,ltrim(rtrim(COLONNA_VALORE)) as COLONNA_VALORE, AMBITO, max(O_CARICO) as O_CARICO from NPLG0_SEMAFORO_MENSILE s where COD_ABI in (0,1025, 3239, 3296, 32334, 3385) and isnull(ID_FILE,'')='' group by COD_ABI,TABELLA,TIPO_CARICAMENTO,PROVENIENZA,COLONNA_VALORE,AMBITO,ID_FILE', NULL, NULL);
--questo per la semaforo domini
"""SELECT max(id) as ID,0 as COD_ABI, FORMAT(DATEADD(MONTH, -1, GETDATE()), 'yyyyMM') AS PERIODO_RIF, tabella, 'Domini' as TIPO_CARICAMENTO, '' as PROVENIENZA,'' as ID_FILE,'' as COLONNA_VALORE, 0 as AMBITO, max(O_CARICO) as O_CARICO FROM [RDBP0_DOMINI].[dbo].[NPLG0_SEMAFORO_DOMINI] group by tabella"""

----------------
INSERT INTO public.tab_task_sources (id, source_id, source_type) VALUES('REAGDG', 'REAGDG', 'JDBC');
INSERT INTO public.tab_task_sources (id, source_id, source_type) VALUES('READVC', 'READVC', 'JDBC');
----------------------
INSERT INTO public.tab_task_destinations (id, destination_id, destination_type) VALUES('REAGDG', 'REAGDG', 'file');
INSERT INTO public.tab_task_destinations (id, destination_id, destination_type) VALUES('READVC', 'READVC', 'file');
-----------------------
INSERT INTO public.tab_jdbc_sources
(source_id, url, username, pwd, driver, tablename, query_text, partitioning_expression, num_partitions)
VALUES('READVC', 'jdbc:sqlserver://pdbclt076.syssede.systest.sanpaoloimi.com\\\\\\\\SYD202:1433;DatabaseName=RDBP0_MENS;encrypt=true;trustServerCertificate=true;integratedSecurity=true;authenticationScheme=NTLM;', 'SYS_LG_RDB@SYSSPIMI', '4EfTw@B9UpCriK#epGiM', 'com.microsoft.sqlserver.jdbc.SQLServerDriver', NULL, 'SELECT BANCA as NUM_BANCA, LTRIM(RTRIM(COD_UO)) as COD_UO, LTRIM(RTRIM(NUM_PARTITA)) as COD_NUM_PARTITA,LTRIM(RTRIM(COD_PRODOTTO)) as COD_PRODOTTO,
LTRIM(RTRIM(TIPO_OPERAZIONE)) as COD_TIPO_OPERAZIONE,LTRIM(RTRIM(CANALE)) as COD_CANALE,LTRIM(RTRIM(PRODOTTO_COMM)) as COD_PRODOTTO_COMM,
LTRIM(RTRIM(PORTAFOGLIO)) as COD_PORTAFOGLIO,LTRIM(RTRIM(DESK_RENDICONTATIVO)) as COD_DESK_RENDICONTATIVO,LTRIM(RTRIM(PTF_SPECCHIO)) as COD_PTF_SPECCHIO,
LTRIM(RTRIM(COD_DATO)) as cod_dato,LTRIM(RTRIM(COD_PROVN_DATI_RED)) as COD_PROVN_DATI_RED, ${num_periodo_rif} as NUM_PERIODO_RIF,${cod_colonna_valore} as IMP_VALORE,
${num_periodo_rif} as NUM_PERIODO_COMP
 FROM READVC with(nolock) where BANCA=${cod_abi} and COD_PROVN_DATI_RED=''${cod_provenienza}''
 and AMBITO = ${num_ambito} and isnull(${cod_colonna_valore},0)<>0', NULL, NULL);
INSERT INTO public.tab_jdbc_sources
(source_id, url, username, pwd, driver, tablename, query_text, partitioning_expression, num_partitions)
VALUES('REAGDG', 'jdbc:sqlserver://pdbclt076.syssede.systest.sanpaoloimi.com\\\\\\\\SYD202:1433;DatabaseName=RDBP0_MENS;encrypt=true;trustServerCertificate=true;integratedSecurity=true;authenticationScheme=NTLM;', 'SYS_LG_RDB@SYSSPIMI', '4EfTw@B9UpCriK#epGiM', 'com.microsoft.sqlserver.jdbc.SQLServerDriver', NULL, 'SELECT BANCA as NUM_BANCA,LTRIM(RTRIM(NDG)) as COD_NDG,LTRIM(RTRIM(INTESTAZIONE)) as COD_INTESTAZIONE,LTRIM(RTRIM(CODICE_FISCALE)) as COD_FISCALE,
LTRIM(RTRIM(COD_TIP_NDG)) as COD_TIP_NDG,LTRIM(RTRIM(COD_PIVA)) as COD_PIVA,LTRIM(RTRIM(COD_STATO_NDG)) as COD_STATO_NDG,
LTRIM(RTRIM(COD_SPECIE_GIURIDICA)) as COD_SPECIE_GIURIDICA, LTRIM(RTRIM(COD_UO_CAPOFILA)) as COD_UO_CAPOFILA, LTRIM(RTRIM(DATA_NASC_COST)) as COD_DATA_NASC_COST,
LTRIM(RTRIM(COD_COMUNE_NASCITA)) as COD_COMUNE_NASCITA, LTRIM(RTRIM(COD_PROVNC_NASCT_COST)) as COD_PROVNC_NASCT_COST,
LTRIM(RTRIM(COD_CAP_RESIDENZA)) as COD_CAP_RESIDENZA, LTRIM(RTRIM(DES_COMUNE_RESIDENZA)) as DES_COMUNE_RESIDENZA,
LTRIM(RTRIM(COD_PROVINCIA_RESIDENZA)) as COD_PROVINCIA_RESIDENZA, LTRIM(RTRIM(COD_SOTTOSEGMENTO_ECONOMICO)) as COD_SOTTOSEGMENTO_ECONOMICO,
LTRIM(RTRIM(COD_SOTTOSEGMENTO_COMMERCIALE)) as COD_SOTTOSEGMENTO_COMMERCIALE, LTRIM(RTRIM(COD_CLI_NOPROFIT)) as COD_CLI_NOPROFIT,
LTRIM(RTRIM(COD_SEGMENTO_ECONOMICO)) as COD_SEGMENTO_ECONOMICO, LTRIM(RTRIM(COD_PTF_CDG)) as COD_PTF_CDG, LTRIM(RTRIM(COD_UTENZA_MODULO)) as COD_UTENZA_MODULO,
LTRIM(RTRIM(COD_SEGM_CDG)) as COD_SEGM_CDG, LTRIM(RTRIM(COD_DESK_CLIENTE)) as COD_DESK_CLIENTE, LTRIM(RTRIM(CLASSE_SOA_UO_PTF)) as COD_CLASSE_SOA_UO_PTF,
LTRIM(RTRIM(COD_MODULO_CDG)) as COD_MODULO_CDG, LTRIM(RTRIM(COD_GESTORE_REMOTO)) as COD_GESTORE_REMOTO, LTRIM(RTRIM(RIF_REGOLA_DESK)) as COD_RIF_REGOLA_DESK,
LTRIM(RTRIM(COD_BU_TERRTRL)) as COD_BU_TERRTRL, LTRIM(RTRIM(COD_TIP_GESTR_GRM)) as COD_TIP_GESTR_GRM, LTRIM(RTRIM(COD_ATECO)) as COD_ATECO,
LTRIM(RTRIM(COD_TIP_GESTR_INDUSTRY)) as COD_TIP_GESTR_INDUSTRY, LTRIM(RTRIM(COD_RAE)) as COD_RAE, LTRIM(RTRIM(COD_BU)) as COD_BU, LTRIM(RTRIM(COD_SAE)) as COD_SAE,
LTRIM(RTRIM(COD_GRM)) as COD_GRM, LTRIM(RTRIM(COD_SESSO)) as COD_SESSO, LTRIM(RTRIM(COD_UTENZA_GRM)) as COD_UTENZA_GRM,
LTRIM(RTRIM(COD_CAPO_INDUSTRY)) as COD_CAPO_INDUSTRY, LTRIM(RTRIM(COD_NDG_PREVLNT)) as COD_NDG_PREVLNT, LTRIM(RTRIM(COD_ABI_SNDG_NDG_PREV)) as COD_ABI_SNDG_NDG_PREV,
LTRIM(RTRIM(COD_SNDG_NDG_PREV)) as COD_SNDG_NDG_PREV, LTRIM(RTRIM(FLG_RESIDENTE)) as COD_FLG_RESIDENTE, LTRIM(RTRIM(COD_RILVNZ_ARTCL_136_TUB)) as COD_RILVNZ_ARTCL_136_TUB,
LTRIM(RTRIM(COD_RILVNZ_ARTCL_53_TUB)) as COD_RILVNZ_ARTCL_53_TUB, LTRIM(RTRIM(COD_RILVNZ_CONSOB)) as COD_RILVNZ_CONSOB, LTRIM(RTRIM(COD_RILVNZ_IAS_24)) as COD_RILVNZ_IAS_24,
LTRIM(RTRIM(COD_NDG_NO_PUSP)) as COD_NDG_NO_PUSP, LTRIM(RTRIM(COD_TIP_GESTN)) as COD_TIP_GESTN, LTRIM(RTRIM(FLG_CAPO_FILR)) as COD_FLG_CAPO_FILR,
LTRIM(RTRIM(FLG_APPRTNZ_FILR)) as COD_FLG_APPRTNZ_FILR, LTRIM(RTRIM(COD_SAG)) as COD_SAG, LTRIM(RTRIM(MACRO_CLUSTER_RATING)) as COD_MACRO_CLUSTER_RATING,
LTRIM(RTRIM(CLUSTER_RATING)) as COD_CLUSTER_RATING, LTRIM(RTRIM(CLASSE_UNICA_RATING)) as COD_CLASSE_UNICA_RATING, LTRIM(RTRIM(PD_MEDIA)) as COD_PD_MEDIA,
LTRIM(RTRIM(PROV_RATING)) as COD_PROV_RATING, LTRIM(RTRIM(COD_COLORE_CLASSE_CRA)) as COD_COLORE_CLASSE_CRA,
LTRIM(RTRIM(COD_CLASSE_RATING_PIU_CRA)) as COD_CLASSE_RATING_PIU_CRA, LTRIM(RTRIM(COD_CDN)) as COD_CDN,
LTRIM(RTRIM(SEGMENTO_REGOLAMENTARE)) as COD_SEGMENTO_REGOLAMENTARE, LTRIM(RTRIM(SEGMENTO_COMPORTAMENTALE)) as COD_SEGMENTO_COMPORTAMENTALE,
LTRIM(RTRIM(FLG_CONDIVISO)) as COD_FLG_CONDIVISO,
case when convert(numeric, data_va) <=20000000 then 20010101 else coalesce(convert(numeric, data_va),20010101) end as NUM_DATA_VA,DATA_INS as NUM_DATA_INS,
PERIODO_RIF as NUM_PERIODO_RIF, ${num_periodo_rif} as NUM_PERIODO_COMP
FROM REAGDG with (nolock)
where BANCA=${cod_abi} and case when convert(int, data_va) <=20000000 then 20010101 else coalesce(convert(int, data_va),20010101) end >=${max_data_va}', 'CONVERT(INT, NUM_DATA_INS/100)', 8);
-----------
INSERT INTO public.tab_file_destinations
(destination_id, format_file, gcs_path, overwrite, csv_separator) VALUES('REAGDG', 'parquet', 'data_output/REAGDG', true, NULL);
INSERT INTO public.tab_file_destinations
(destination_id, format_file, gcs_path, overwrite, csv_separator) VALUES('READVC', 'parquet', 'data_output/READVC', true, NULL);