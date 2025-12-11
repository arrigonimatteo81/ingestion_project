drop table if exists public.tab_file_sources;

CREATE TABLE public.tab_file_sources (
	source_id varchar NOT NULL,
	file_type varchar NOT NULL,
	path varchar NOT NULL,
	excel_sheet varchar NULL,
	csv_separator varchar NULL,
	CONSTRAINT check_one_column_populated CHECK (((((file_type)::text = 'EXCEL'::text) AND (excel_sheet IS NOT NULL)) OR (((file_type)::text = 'CSV'::text) AND (csv_separator IS NOT NULL)))),
	CONSTRAINT tab_file_sources_pk PRIMARY KEY (source_id),
	CONSTRAINT chk_file_type CHECK (((file_type)::text = ANY ((ARRAY['EXCEL'::character varying, 'PARQUET'::character varying, 'AVRO'::character varying, 'CSV'::character varying,'excel'::character varying, 'parquet'::character varying, 'avro'::character varying, 'csv'::character varying])::text[])))
);

ALTER TABLE public.tab_file_sources ADD CONSTRAINT fk_tab_file_sources FOREIGN KEY (source_id) REFERENCES public.tab_task_sources(source_id) ON DELETE CASCADE ON UPDATE CASCADE;
