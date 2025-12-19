DROP table if exists public.tab_task_logs;

CREATE TABLE public.tab_task_logs (
	task_id varchar NOT NULL,
	run_id varchar NOT NULL,
	state_id varchar NOT NULL,
	description varchar NULL,
	error_message varchar NULL,
	update_ts timestamp DEFAULT now() NOT NULL,
	task_group varchar DEFAULT ''::character varying NOT NULL,
	rows_affected int4 NULL
);

GRANT SELECT, INSERT, UPDATE ON table public.tab_task_logs TO utente;