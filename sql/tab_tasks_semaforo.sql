DROP TABLE if exists public.tab_tasks_semaforo;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE public.tab_tasks_semaforo (
    uid uuid DEFAULT gen_random_uuid(),
	id int4 NOT NULL,
	cod_abi int4 NULL,
	source_id varchar not null REFERENCES public.tab_task_sources(source_id) ON DELETE RESTRICT,
	destination_id varchar not null REFERENCES public.tab_task_destinations(destination_id) ON DELETE RESTRICT, ,
	cod_provenienza varchar null,
	num_periodo_rif int4 null,
	cod_gruppo varchar null,
	cod_colonna_valore varchar null,
	num_ambito int4 null,
	num_max_data_va bigint,
	CONSTRAINT tab_tasks_semaforo_pk PRIMARY KEY (uid)
);

GRANT SELECT,insert,update,delete,truncate ON table public.tab_tasks_semaforo TO fdir_app;