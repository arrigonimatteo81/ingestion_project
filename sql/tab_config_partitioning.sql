drop table if exists public.tab_config_partitioning;

CREATE TABLE public.tab_config_partitioning (
	"key" jsonb NOT NULL,
	partitioning_expression varchar NULL,
	num_partitions int4 NULL,
	CONSTRAINT check_partitioning_both_or_none_cols CHECK ((((partitioning_expression IS NOT NULL) AND (num_partitions IS NOT NULL)) OR ((partitioning_expression IS NULL) AND (num_partitions IS NULL)))),
	CONSTRAINT tab_config_partitioning_pk PRIMARY KEY ("key")
);


GRANT SELECT ON table public.tab_config_partitioning TO nplg_app;

INSERT INTO public.tab_config_partitioning
("key", partitioning_expression, num_partitions)
VALUES
('{"cod_abi": 1025, "cod_tabella": "REAGDG", "cod_provenienza": "AN"}', 'ABS(CHECKSUM(NDG)) % 15', 15),
('{"cod_abi": 3296, "cod_tabella": "REAGDG", "cod_provenienza": "AN"}', 'ABS(CHECKSUM(NDG)) % 3', 3),
('{"cod_abi": 1025, "cod_tabella": "READDR", "cod_provenienza": "N1"}', 'ABS(CHECKSUM(concat(cod_uo,NUM_PARTITA))) % 10', 10),
('{"cod_abi": 1025, "cod_tabella": "READDR", "cod_provenienza": "M1"}', 'ABS(CHECKSUM(concat(cod_uo,NUM_PARTITA))) % 5',5),
('{"cod_abi": 3296, "cod_tabella": "READDR", "cod_provenienza": "DP"}', 'ABS(CHECKSUM(concat(cod_uo,NUM_PARTITA))) % 5',5)
;

