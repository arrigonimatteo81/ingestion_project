drop table if exists public.tab_task_sources;

CREATE TABLE public.tab_task_sources (
	id varchar NOT NULL,
    source_id varchar NOT null UNIQUE,
	source_type varchar NOT NULL,
	CONSTRAINT chk_source_type CHECK (((source_type)::text = ANY ((ARRAY['jdbc'::character varying, 'bigquery'::character varying, 'file'::character varying, 'JDBC'::character varying, 'BIGQUERY'::character varying, 'FILE'::character varying])::text[]))),
	CONSTRAINT tab_task_sources_pk PRIMARY KEY (source_id)
);

--ALTER TABLE public.tab_task_sources ADD CONSTRAINT fk_tab_task_sources FOREIGN KEY (source_id) REFERENCES public.tab_tasks(source_id) ON DELETE CASCADE ON UPDATE CASCADE;

GRANT SELECT ON table public.tab_task_sources TO nplg_app;

INSERT INTO public.tab_task_sources
(id, source_id, source_type)
VALUES(
('REAGDG', 'REAGDG', 'JDBC'),
('READDR', 'READDR', 'JDBC')
);