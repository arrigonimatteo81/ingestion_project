INSERT INTO public.tab_tasks
(id, source_id, destination_id, description, config_profile, is_blocking)
VALUES('task_cutomers', 'customer_source', 'customer_destination', 'ingestion tabella customer', 'config_1', false);

INSERT INTO public.tab_task_destinations
(id, destination_id, destination_type)
VALUES('id1', 'customer_destination', 'file');

INSERT INTO public.tab_file_destinations
(destination_id, format_file, gcs_path)
VALUES('customer_destination', 'csv', '/data_output');

INSERT INTO public.tab_task_sources
(id, source_id, source_type)
VALUES('id1', 'customer_source', 'jdbc');

INSERT INTO public.tab_jdbc_sources
(source_id, url, username, pwd, driver, tablename, query_text, partitioning_expression, num_partitions)
VALUES('customer_source', 'mettiamoci_un_url', 'user', 'user2025', 'org.mysql.com', 'cutomers', NULL, NULL, NULL);
