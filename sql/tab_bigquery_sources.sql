drop table if exists public.tab_bigquery_sources;

CREATE TABLE public.tab_bigquery_sources (
	source_id varchar NOT NULL,
	project varchar NOT NULL,
	dataset varchar NOT NULL,
	tablename varchar NULL,
	query_text varchar NULL,
	CONSTRAINT check_one_column_populated CHECK ((((tablename IS NOT NULL) AND (query_text IS NULL)) OR ((tablename IS NULL) AND (query_text IS NOT NULL)))),
	CONSTRAINT tab_bigquery_sources_pk PRIMARY KEY (source_id)
);

ALTER TABLE public.tab_bigquery_sources ADD CONSTRAINT fk_tab_bigquery_sources FOREIGN KEY (source_id) REFERENCES public.tab_task_sources(source_id) ON DELETE CASCADE ON UPDATE CASCADE;

GRANT SELECT ON table public.tab_bigquery_sources TO fdir_app;