DROP TABLE if exists public.tab_tasks;

CREATE TABLE public.tab_tasks (
	id varchar NOT NULL,
	source_id varchar not null unique,
	destination_id varchar not null unique,
	description varchar NULL,
	config_profile varchar NOT NULL,
	is_blocking bool NULL,
	CONSTRAINT tab_tasks_pk PRIMARY KEY (id)
);


-- public.tasks foreign keys

ALTER TABLE public.tab_tasks ADD CONSTRAINT tab_tasks_config_fk FOREIGN KEY (config_profile) REFERENCES public.tab_task_configs("name") ON DELETE CASCADE ON UPDATE CASCADE;

GRANT SELECT ON table public.tab_tasks TO utente;