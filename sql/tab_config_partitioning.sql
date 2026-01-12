drop table if exists public.tab_config_partitioning;

CREATE TABLE public.tab_config_partitioning (
	"key" jsonb NOT NULL,
	partitioning_expression varchar NULL,
	num_partitions int4 NULL,
	CONSTRAINT check_partitioning_both_or_none_cols CHECK ((((partitioning_expression IS NOT NULL) AND (num_partitions IS NOT NULL)) OR ((partitioning_expression IS NULL) AND (num_partitions IS NULL)))),
	CONSTRAINT tab_config_partitioning_pk PRIMARY KEY ("key")
);


GRANT SELECT ON table public.tab_jdbc_sources TO fdir_app;

