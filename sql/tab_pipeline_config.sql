DROP TABLE if exists public.tab_pipeline_configs;

CREATE TABLE public.tab_semaforo_ready (
	uid uuid NULL,
	source_id varchar(128) NULL,
	destination_id varchar(128) NULL,
	tipo_caricamento varchar NULL,
	"key" jsonb NULL,
	query_param jsonb NULL,
	is_heavy bool
);