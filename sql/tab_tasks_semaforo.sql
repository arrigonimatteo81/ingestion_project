DROP TABLE if exists public.tab_tasks_semaforo;

CREATE TABLE public.tab_tasks_semaforo (
	id int4 NOT NULL,
	cod_abi int4 NULL,
	source_id varchar not null,
	destination_id varchar not null,
	cod_provenienza varchar null,
	num_periodo_rif int4 null,
	cod_gruppo varchar null,
	cod_colonna_valore varchar null,
	num_ambito int4 null,
	num_max_data_va integer,
	CONSTRAINT tab_tasks_semaforo_pk PRIMARY KEY (id)
);

GRANT SELECT,insert,update,delete,truncate ON table public.tab_tasks_semaforo TO fdir_app;
