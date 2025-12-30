drop table if exists public.tab_task_destinations;

CREATE TABLE public.tab_task_destinations (
	id varchar NOT NULL,
    destination_id varchar NOT null UNIQUE,
	destination_type varchar NOT NULL,
	CONSTRAINT chk_destination_type CHECK (((destination_type)::text = ANY ((ARRAY['jdbc'::character varying, 'bigquery'::character varying, 'file'::character varying, 'JDBC'::character varying, 'BIGQUERY'::character varying, 'FILE'::character varying])::text[]))),
	CONSTRAINT tab_task_destinations_pk PRIMARY KEY (destination_id)
);


--ALTER TABLE public.tab_task_destinations ADD CONSTRAINT fk_tab_task_destinations FOREIGN KEY (destination_id) REFERENCES public.tab_tasks(destination_id) ON DELETE CASCADE ON UPDATE CASCADE;

GRANT SELECT ON table public.tab_task_destinations TO utente;