-- public.task_transformations_gcs_source definition

-- Drop table

-- DROP TABLE public.task_transformations_gcs_source;

CREATE TABLE public.tab_file_sources (
	source_id varchar NOT NULL,
	file_type varchar NOT NULL CHECK (file_type in ('EXCEL','CSV','PARQUET')),
	path varchar NOT NULL,
	excel_sheet varchar NULL,
	csv_separator varchar NULL,
	CONSTRAINT check_one_column_populated CHECK (((((file_type)::text = 'EXCEL'::text) AND (excel_sheet IS NOT NULL)) OR (((file_type)::text = 'CSV'::text) AND (csv_separator IS NOT NULL)))),
	CONSTRAINT tab_file_sources_pk PRIMARY KEY (source_id)
	CONSTRAINT chk_file_type CHECK (((file_type)::text = ANY ((ARRAY['EXCEL'::character varying, 'PARQUET'::character varying, 'AVRO'::character varying, 'CSV'::character varying])::text[]))),
);


-- public.task_transformations_gcs_source foreign keys

--ALTER TABLE public.task_transformations_gcs_source ADD CONSTRAINT fk_task_transformations_gcs_source FOREIGN KEY (source_id,transformation_id) REFERENCES public.task_transformations_sources(id,transformation_id) ON DELETE CASCADE ON UPDATE CASCADE;