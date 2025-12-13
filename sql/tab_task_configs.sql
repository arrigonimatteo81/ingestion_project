DROP TABLE if exists public.tab_task_configs;

CREATE TABLE public.tab_task_configs (
	"name" varchar NOT NULL,
	description varchar NULL,
	main_python_file varchar NOT NULL,
	additional_python_file_uris _varchar NULL,
	jar_file_uris _varchar NULL,
	additional_file_uris _varchar NULL,
	archive_file_uris _varchar NULL,
	logging_config json NULL,
	dataproc_properties json NULL,
	processor_type varchar DEFAULT 'spark'::character varying NOT NULL,
	CONSTRAINT tab_task_configs_pk PRIMARY KEY (name),
	CONSTRAINT tab_task_configs_processor_type_check CHECK (((processor_type)::text = ANY ((ARRAY['spark'::character varying, 'bigquery'::character varying, 'SPARK'::character varying, 'BIGQUERY'::character varying])::text[])))
);


GRANT SELECT ON table public.tab_task_configs TO utente;