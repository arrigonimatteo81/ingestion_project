drop table if exists public.tab_file_destinations;

CREATE TABLE public.tab_file_destinations (
	destination_id varchar NOT NULL,
	format_file varchar NOT NULL,
	gcs_path varchar NOT NULL,
	overwrite bool NULL,
	csv_separator varchar NULL,
	CONSTRAINT check_one_column_populated CHECK (((((format_file)::text = ANY ((ARRAY['csv'::character varying, 'CSV'::character varying])::text[])) AND (csv_separator IS NOT NULL)) OR (((format_file)::text = ANY ((ARRAY['parquet'::character varying, 'PARQUET'::character varying])::text[])) AND (csv_separator IS NULL)) OR (((format_file)::text = ANY ((ARRAY['avro'::character varying, 'AVRO'::character varying])::text[])) AND (csv_separator IS NULL)) OR (((format_file)::text = ANY ((ARRAY['excel'::character varying, 'EXCEL'::character varying])::text[])) AND (csv_separator IS NULL)))),
	CONSTRAINT chk_format_file CHECK (((format_file)::text = ANY (ARRAY[('excel'::character varying)::text, ('parquet'::character varying)::text, ('avro'::character varying)::text, ('csv'::character varying)::text, ('EXCEL'::character varying)::text, ('PARQUET'::character varying)::text, ('AVRO'::character varying)::text, ('CSV'::character varying)::text]))),
	CONSTRAINT tab_destinations_file_pk PRIMARY KEY (destination_id)
);

-- public.tab_file_destinations foreign keys

ALTER TABLE public.tab_file_destinations ADD CONSTRAINT fk_tab_file_destinations FOREIGN KEY (destination_id) REFERENCES public.tab_task_destinations(destination_id) ON DELETE CASCADE ON UPDATE CASCADE;
GRANT SELECT ON table public.tab_file_destinations TO nplg_app;

INSERT INTO public.tab_file_destinations
(destination_id, format_file, gcs_path, overwrite, csv_separator)
VALUES
('REAGDG_STG', 'parquet', 'gs://bkt-isp-nplg0-dpstg-svil-001-ew12/staging/REAGDG_STG', false, NULL),
('READDR_STG', 'parquet', 'gs://bkt-isp-nplg0-dpstg-svil-001-ew12/staging/READDR_STG', false, NULL)
;