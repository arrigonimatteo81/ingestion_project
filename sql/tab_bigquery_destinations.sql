drop table if exists public.tab_bigquery_destinations;

CREATE TABLE public.tab_bigquery_destinations (
	destination_id varchar NOT NULL,
	project varchar NOT NULL,
	dataset varchar NOT NULL,
	tablename varchar NOT NULL,
	gcs_bucket varchar NULL,
	use_direct_write bool NULL,
	columns  JSONB,
	overwrite bool NULL,
	CONSTRAINT check_one_column_populated CHECK ((((gcs_bucket IS NULL) AND (use_direct_write = true)) OR ((gcs_bucket IS NOT NULL) AND (use_direct_write = false)))),
    CONSTRAINT tab_bigquery_destinations_pk PRIMARY KEY (destination_id)
);

ALTER TABLE public.tab_bigquery_destinations ADD CONSTRAINT fk_tab_bigquery_destinations FOREIGN KEY (destination_id) REFERENCES public.tab_task_destinations(destination_id) ON DELETE CASCADE ON UPDATE CASCADE;

GRANT SELECT ON table public.tab_bigquery_destinations TO nplg_app;