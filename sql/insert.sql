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
