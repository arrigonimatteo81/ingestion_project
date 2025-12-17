INSERT INTO public.tab_task_configs ("name",description,main_python_file,additional_python_file_uris,jar_file_uris,additional_file_uris,archive_file_uris,logging_config,dataproc_properties,processor_type) VALUES
	 ('config1','configurazione di test 1','main_processor.py','{}','{jar/postgresql-42.7.8.jar}','{}','{}',NULL,NULL,'spark');

INSERT INTO public.tab_task_group (task_id,group_name) VALUES
	 ('task_test_1','TEST_GROUP'),
	 ('task_test_2','TEST_GROUP_2'),
	 ('task_test_3','TEST_GROUP_3');
-----------------------------------------------------------------------------------------------------------------
INSERT INTO public.tab_tasks
(id, source_id, destination_id, description, config_profile, is_blocking)
VALUES('task_test_1', 'bqcommands_source', 'my_destination', 'ingestion tabella bq_commands', 'config1', false);

INSERT INTO public.tab_task_destinations
(id, destination_id, destination_type)
VALUES('id1', 'my_destination', 'file');

INSERT INTO public.tab_file_destinations
(destination_id, format_file, gcs_path)
VALUES('my_destination', 'csv', 'data_output/my_destination');

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

INSERT INTO public.tab_task_destinations
(id, destination_id, destination_type)
VALUES('id2', 'csv_destination', 'file');

INSERT INTO public.tab_file_destinations
(destination_id, format_file, gcs_path, overwrite)
VALUES('csv_destination', 'csv', 'data_output/my_new_destination', False);

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
---------------------------------------------------------------------------------------------------------------
