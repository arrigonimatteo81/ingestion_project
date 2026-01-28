-- public.tab_table_configs definition

-- Drop table

DROP TABLE if exists public.tab_table_configs;

CREATE TABLE public.tab_table_configs (
	logical_table varchar NOT NULL,
	real_table varchar NULL,
	processor_type varchar DEFAULT 'spark'::character varying NOT NULL,
	layer varchar NOT NULL,
	has_next bool NULL,
	is_blocking bool NULL,
	CONSTRAINT tab_table_configs_layer_check CHECK (((layer)::text = ANY (ARRAY[('stage'::character varying)::text, ('silver'::character varying)::text, ('STAGE'::character varying)::text, ('SILVER'::character varying)::text]))),
	CONSTRAINT tab_table_configs_pk PRIMARY KEY (logical_table, layer),
	CONSTRAINT tab_table_configs_processor_type_check CHECK (((processor_type)::text = ANY (ARRAY[('spark'::character varying)::text, ('bigquery'::character varying)::text, ('SPARK'::character varying)::text, ('BIGQUERY'::character varying)::text, ('NATIVE'::character varying)::text, ('native'::character varying)::text])))
);