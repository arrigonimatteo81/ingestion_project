DROP TABLE if exists public.tab_task_configs;

CREATE TABLE public.tab_task_configs (
	"key" jsonb NOT NULL,
	description varchar NULL,
	main_python_file varchar NOT NULL,
	additional_python_file_uris _varchar NULL,
	jar_file_uris _varchar NULL,
	additional_file_uris _varchar NULL,
	archive_file_uris _varchar NULL,
	logging_config json NULL,
	dataproc_properties json NULL,
	processor_type varchar DEFAULT 'spark'::character varying NOT NULL,
	is_heavy bool DEFAULT false NULL,
	CONSTRAINT tab_task_configs_pk PRIMARY KEY (key),
	CONSTRAINT tab_task_configs_processor_type_check CHECK (((processor_type)::text = ANY (ARRAY[('spark'::character varying)::text, ('bigquery'::character varying)::text, ('SPARK'::character varying)::text, ('BIGQUERY'::character varying)::text, ('NATIVE'::character varying)::text, ('native'::character varying)::text])))
);


GRANT SELECT ON table public.tab_task_configs TO nplg0_app;

INSERT INTO public.tab_task_configs
("key", description, main_python_file, jar_file_uris, dataproc_properties, processor_type, is_heavy)
VALUES
('{"cod_abi": 1025, "cod_tabella": "REAGDG", "cod_provenienza": "AN"}', 'configurazione per REAGDG banca 1025', 'main_processor.py',
 '{jar/mssql-jdbc-12.2.0.jre11.jar}', '{ "spark.executor.memory": "6G", "spark.driver.memory": "4G", "spark.executor.cores": "4","spark.executor.instances": "4", "spark.ui.enabled" : "true", "spark.sql.adaptive.enabled": "true", "spark.dynamicAllocation.initialExecutors": "3", "spark.sql.shuffle.partitions": "20"}',
 'spark', true),
 ('{"cod_abi": 1025, "cod_tabella": "READDR", "cod_provenienza": "N1"}', 'configurazione per READDR banca 1025 provenienza N1', 'main_processor.py',
 '{jar/mssql-jdbc-12.2.0.jre11.jar}', '{ "spark.executor.memory": "6G", "spark.driver.memory": "4G", "spark.executor.cores": "4","spark.executor.instances": "4", "spark.ui.enabled" : "true", "spark.sql.adaptive.enabled": "true", "spark.dynamicAllocation.initialExecutors": "3", "spark.sql.shuffle.partitions": "20"}',
 'spark', true),
 ('{"cod_abi": 3296, "cod_tabella": "READDR", "cod_provenienza": "DP"}', 'configurazione per READDR banca 3296 provenienza DP', 'main_processor.py',
 '{jar/mssql-jdbc-12.2.0.jre11.jar}', '{ "spark.executor.memory": "6G", "spark.driver.memory": "4G", "spark.executor.cores": "4","spark.executor.instances": "4", "spark.ui.enabled" : "true", "spark.sql.adaptive.enabled": "true", "spark.dynamicAllocation.initialExecutors": "3", "spark.sql.shuffle.partitions": "20"}',
 'spark', true),
('{}','configurazione di default','main_processor.py','{jar/mssql-jdbc-12.2.0.jre11.jar,jar/postgresql-42.7.8.jar}',NULL,'spark',false)
