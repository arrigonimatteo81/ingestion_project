
INSERT INTO public.tab_task_configs ("name",description,main_python_file,additional_python_file_uris,jar_file_uris,additional_file_uris,archive_file_uris,logging_config,dataproc_properties,processor_type) VALUES
	 ('config1','configurazione di test 1','main_processor.py','{}','{jar/postgresql-42.7.8.jar}','{}','{}',NULL,NULL,'spark');
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
INSERT INTO public.tab_tasks
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
VALUES('semaforo_source', 'jdbc:sqlserver://pdbclt076.syssede.systest.sanpaoloimi.com\\\\SYD202:1433;DatabaseName=RDBP0_MENS;encrypt=true;trustServerCertificate=true;integratedSecurity=true;authenticationScheme=NTLM;', 'SYS_LG_RDB@SYSSPIMI', '4EfTw@B9UpCriK#epGiM', 'com.microsoft.sqlserver.jdbc.SQLServerDriver', NULL, 'select max(id) as ID,COD_ABI,PERIODO_RIF,TABELLA,TIPO_CARICAMENTO,PROVENIENZA,ID_FILE,ltrim(rtrim(COLONNA_VALORE)) as COLONNA_VALORE, AMBITO, max(O_CARICO) as O_CARICO from NPLG0_SEMAFORO_MENSILE s where COD_ABI in (0,1025, 3239, 3296, 32334, 3385) and isnull(ID_FILE,'')='' group by COD_ABI,PERIODO_RIF,TABELLA,TIPO_CARICAMENTO,PROVENIENZA,COLONNA_VALORE,AMBITO,ID_FILE', NULL, NULL);