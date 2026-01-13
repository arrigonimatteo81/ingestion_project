DROP table if exists public.tab_task_logs;

CREATE TABLE public.tab_task_logs (
	"key" jsonb NOT NULL,
	periodo int4 NULL,
	run_id varchar NOT NULL,
	state_id varchar NOT NULL,
	description varchar NULL,
	error_message varchar NULL,
	update_ts timestamp DEFAULT now() NOT NULL,
	rows_affected int4 NULL
);

GRANT SELECT, INSERT, UPDATE ON table public.tab_task_logs TO utente;


CREATE OR REPLACE VIEW public.vw_task_logs as
select
tab_inizi.cod_tabella,
tab_inizi.cod_abi,
tab_inizi.cod_provenienza,
periodo,
tab_inizi.run_id,
tab_inizi.description,
tab_fini.error_message,
tab_fini.rows_affected,
tab_inizi.update_ts as tms_inizio,
tab_fini.update_ts as tms_fine
from
(select
	key,
	key ->> 'cod_tabella'       AS cod_tabella,
    LPAD(key ->> 'cod_abi', 5, '0') as cod_abi,
    key ->> 'cod_provenienza'   AS cod_provenienza,
    run_id ,
	'START' as state_id ,
	description ,
	error_message ,
	update_ts ,
	rows_affected
FROM public.tab_task_logs
where state_id = 'RUNNING') as tab_inizi
left join
(select
	key,
	key ->> 'cod_tabella'       AS cod_tabella,
    LPAD(key ->> 'cod_abi', 5, '0') as cod_abi,
    key ->> 'cod_provenienza'   AS cod_provenienza,
    periodo,
    run_id ,
	state_id ,
	description ,
	error_message ,
	update_ts ,
	rows_affected
FROM public.tab_task_logs
where state_id != 'RUNNING') as tab_fini
on tab_inizi.key=tab_fini.key