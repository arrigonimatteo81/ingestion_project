DROP TABLE if exists public.tab_semaforo_steps;

CREATE TABLE public.tab_semaforo_steps (
	uid uuid NULL,
	run_id varchar NOT NULL,
	logical_table varchar(128) NOT NULL,
	tipo_caricamento varchar NOT NULL,
	"key" jsonb NOT NULL,
	query_param jsonb NOT NULL,
	layer varchar NULL
);

GRANT TRUNCATE, INSERT, select ON public.tab_semaforo_steps TO nplg_app;