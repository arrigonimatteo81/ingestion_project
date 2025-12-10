-- public.task_destinations_gcs definition

-- Drop table

-- DROP TABLE public.task_destinations_gcs;

CREATE TABLE public.tab_destinations_file (
	id_destination varchar NOT NULL,
	format_file varchar NOT NULL,
	gcs_path varchar NOT NULL,
	CONSTRAINT chk_format_file CHECK (((format_file)::text = ANY ((ARRAY['excel'::character varying, 'parquet'::character varying, 'avro'::character varying, 'csv'::character varying, 'EXCEL'::character varying, 'PARQUET'::character varying, 'AVRO'::character varying, 'CSV'::character varying])::text[]))),
	CONSTRAINT tab_destinations_file_pk PRIMARY KEY (id_destination)
);


-- public.task_destinations_gcs foreign keys

--ALTER TABLE public.task_destinations_gcs ADD CONSTRAINT task_destinations_gcs_task_destinations_fk FOREIGN KEY (task_destination_id) REFERENCES public.task_destinations(id) ON DELETE CASCADE ON UPDATE CASCADE;