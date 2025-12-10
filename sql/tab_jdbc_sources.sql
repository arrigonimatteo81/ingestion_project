-- public.task_transformations_jdbc_source definition

-- Drop table

-- DROP TABLE public.task_transformations_jdbc_source;

CREATE TABLE public.tab_jdbc_sources (
	source_id varchar NOT NULL,
	url varchar NOT NULL,
	username varchar NOT NULL,
	pwd varchar NOT NULL,
	driver varchar NOT NULL,
	tablename varchar NULL,
	query_text varchar NULL,
	partitioning_expression varchar NULL,
	num_partitions int4 NULL,
	CONSTRAINT check_one_column_populated CHECK ((((tablename IS NOT NULL) AND (query_text IS NULL)) OR ((tablename IS NULL) AND (query_text IS NOT NULL)))),
	CONSTRAINT check_partitioning_both_or_none_cols CHECK ((((partitioning_expression IS NOT NULL) AND (num_partitions IS NOT NULL)) OR ((partitioning_expression IS NULL) AND (num_partitions IS NULL)))),
	CONSTRAINT tab_jdbc_sources_pk PRIMARY KEY (source_id)
);


-- public.task_transformations_jdbc_source foreign keys

--ALTER TABLE public.task_transformations_jdbc_source ADD CONSTRAINT task_trans_jdbc_source_task_transformations_sources_fk FOREIGN KEY (source_id,transformation_id) REFERENCES public.task_transformations_sources(id,transformation_id) ON DELETE CASCADE ON UPDATE CASCADE;