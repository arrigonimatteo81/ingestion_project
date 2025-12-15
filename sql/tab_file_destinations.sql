drop table if exists public.tab_file_destinations;

CREATE TABLE public.tab_file_destinations (
	destination_id varchar NOT NULL,
	format_file varchar NOT NULL,
	gcs_path varchar NOT NULL,
	overwrite bool,
	csv_separator varchar NOT NULL
	#TODO regola se CSV serve csv_separator
	CONSTRAINT chk_format_file CHECK (((format_file)::text = ANY ((ARRAY['excel'::character varying, 'parquet'::character varying, 'avro'::character varying, 'csv'::character varying, 'EXCEL'::character varying, 'PARQUET'::character varying, 'AVRO'::character varying, 'CSV'::character varying])::text[]))),
	CONSTRAINT tab_destinations_file_pk PRIMARY KEY (destination_id)
);

ALTER TABLE public.tab_file_destinations ADD CONSTRAINT fk_tab_file_destinations FOREIGN KEY (destination_id) REFERENCES public.tab_task_destinations(destination_id) ON DELETE CASCADE ON UPDATE CASCADE;

GRANT SELECT ON table public.tab_file_destinations TO utente;