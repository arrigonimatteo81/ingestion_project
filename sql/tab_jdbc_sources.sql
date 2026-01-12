drop table if exists public.tab_jdbc_sources;

CREATE TABLE public.tab_jdbc_sources (
	source_id varchar NOT NULL,
	url varchar NOT NULL,
	username varchar NOT NULL,
	pwd varchar NOT NULL,
	driver varchar NOT NULL,
	tablename varchar NULL,
	query_text varchar NULL,
	CONSTRAINT check_one_column_populated CHECK ((((tablename IS NOT NULL) AND (query_text IS NULL)) OR ((tablename IS NULL) AND (query_text IS NOT NULL)))),
	CONSTRAINT tab_jdbc_sources_pk PRIMARY KEY (source_id)
);

ALTER TABLE public.tab_jdbc_sources ADD CONSTRAINT fk_tab_jdbc_sources FOREIGN KEY (source_id) REFERENCES public.tab_task_sources(source_id) ON DELETE CASCADE ON UPDATE CASCADE;

GRANT SELECT ON table public.tab_jdbc_sources TO utente;