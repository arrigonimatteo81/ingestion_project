DROP TABLE public.tab_table_configs;

CREATE TABLE public.tab_table_configs (
	logical_table varchar NOT NULL,
	stage varchar NULL,
	silver varchar NULL,
	stage_processor_type varchar DEFAULT 'spark'::character varying NOT NULL,
	silver_processor_type varchar DEFAULT 'bigquery'::character varying NOT NULL,
	CONSTRAINT tab_table_configs_pk PRIMARY KEY (logical_table),
	CONSTRAINT tab_table_configs_stage_processor_type_check CHECK (((stage_processor_type)::text = ANY (ARRAY[('spark'::character varying)::text, ('bigquery'::character varying)::text, ('SPARK'::character varying)::text, ('BIGQUERY'::character varying)::text, ('NATIVE'::character varying)::text, ('native'::character varying)::text]))),
	CONSTRAINT tab_table_configs_silver_processor_type_check CHECK (((stage_processor_type)::text = ANY (ARRAY[('spark'::character varying)::text, ('bigquery'::character varying)::text, ('SPARK'::character varying)::text, ('BIGQUERY'::character varying)::text, ('NATIVE'::character varying)::text, ('native'::character varying)::text])))
);

GRANT SELECT ON table public.tab_table_configs TO nplg_app;