drop table if exists public.tab_semaforo_mensile;

CREATE TABLE public.tab_semaforo_mensile (
	id int4 NOT NULL,
	cod_abi int4 NULL,
	periodo_rif int4 NULL,
	tabella varchar(128) NOT NULL,
	tipo_caricamento varchar(8) NOT NULL,
	provenienza bpchar(2) NOT NULL,
	id_file varchar(32) NULL,
	colonna_valore varchar(16) NULL,
	ambito int4 NULL,
	o_carico timestamp(3) NULL,
	CONSTRAINT pk_tab_semaforo_mensile PRIMARY KEY (id)
);


GRANT TRUNCATE, INSERT, SELECT ON public.tab_semaforo_mensile TO nplg_app;


INSERT INTO public.tab_semaforo_mensile
(id, cod_abi, periodo_rif, tabella, tipo_caricamento, provenienza, id_file, colonna_valore, ambito, o_carico)
VALUES
(1, 1025, 202601, 'REAGDG', 'Clienti', 'AN', '', '', 0, CURRENT_TIMESTAMP), --heavy
(2, 3239, 202601, 'REAGDG', 'Clienti', 'AN', '', '', 0, CURRENT_TIMESTAMP),
(3, 3296, 202601, 'REAGDG', 'Clienti', 'AN', '', '', 0, CURRENT_TIMESTAMP),
(4, 3385, 202601, 'REAGDG', 'Clienti', 'AN', '', '', 0, CURRENT_TIMESTAMP),
(5, 32334, 202601, 'REAGDG', 'Clienti', 'AN', '', '', 0, CURRENT_TIMESTAMP),
(6, 1025, 202601, 'READDR', 'Rapporti', 'N1', '', '', 0, CURRENT_TIMESTAMP), --heavy
(7, 1025, 202601, 'READDR', 'Rapporti', 'M1', '', '', 0, CURRENT_TIMESTAMP),
(8, 3296, 202601, 'READDR', 'Rapporti', 'DP', '', '', 0, CURRENT_TIMESTAMP), --heavy
(9, 3385, 202601, 'READDR', 'Rapporti', 'VR', '', '', 0, CURRENT_TIMESTAMP),
(10, 32334, 202601, 'READDR', 'Rapporti', 'P0', '', '', 0, CURRENT_TIMESTAMP)
;


DROP TABLE if exists public.tab_task_configs;

CREATE TABLE public.tab_task_configs (
	"key" jsonb NOT NULL,
	description varchar NULL,
	main_python_file varchar NOT NULL,
	additional_python_file_uris _varchar NULL,
	jar_file_uris _varchar NULL,
	additional_file_uris _varchar NULL,
	archive_file_uris _varchar NULL,
	logging_config json NULL,
	dataproc_properties json NULL,
	processor_type varchar DEFAULT 'spark'::character varying NOT NULL,
	is_heavy bool DEFAULT false NULL,
	CONSTRAINT tab_task_configs_pk PRIMARY KEY (key),
	CONSTRAINT tab_task_configs_processor_type_check CHECK (((processor_type)::text = ANY (ARRAY[('spark'::character varying)::text, ('bigquery'::character varying)::text, ('SPARK'::character varying)::text, ('BIGQUERY'::character varying)::text, ('NATIVE'::character varying)::text, ('native'::character varying)::text])))
);


GRANT SELECT ON table public.tab_task_configs TO nplg_app;

INSERT INTO public.tab_task_configs
("key", description, main_python_file, jar_file_uris, dataproc_properties, processor_type, is_heavy)
VALUES
('{"cod_abi": 1025, "cod_tabella": "READDR", "cod_provenienza": "N1"}'::jsonb, 'configurazione per READDR banca 1025 provenienza N1', 'gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/main_processor.py', '{gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/ingestion_process-1.0.0-py3-none-any.whl}', '{gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/jar/mssql-jdbc-12.2.0.jre11.jar,gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/jar/postgresql-42.7.8.jar}', NULL, NULL, NULL, '{ "spark.driver.cores" : "2","spark.driver.memory": "4g", "spark.executor.memory": "12g", "spark.executor.cores": "4",
 "spark.default.parallelism": "12","spark.ui.enabled" : "true", "spark.sql.adaptive.enabled": "true", "spark.sql.adaptive.coalescePartitions.enabled":"true", 
"spark.sql.adaptive.skewJoin.enabled":"true","spark.sql.shuffle.partitions": "12","spark.dynamicAllocation.enabled":"true","spark.shuffle.service.enabled":"true",
"spark.dynamicAllocation.minExecutors":"1","spark.dynamicAllocation.initialExecutors":"3","spark.dynamicAllocation.maxExecutors":"5","spark.submit.deployMode":"cluster"}'::json, 'spark', true),
('{"cod_abi": 1025, "cod_tabella": "REAGDG", "cod_provenienza": "AN"}'::jsonb, 'configurazione per REAGDG banca 1025', 'gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/main_processor.py', '{gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/ingestion_process-1.0.0-py3-none-any.whl}', '{gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/jar/mssql-jdbc-12.2.0.jre11.jar,gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/jar/postgresql-42.7.8.jar}', NULL, NULL, NULL, '{ "spark.driver.cores" : "2","spark.driver.memory": "4g", "spark.executor.memory": "12g", "spark.executor.cores": "4",
 "spark.default.parallelism": "12","spark.ui.enabled" : "true", "spark.sql.adaptive.enabled": "true", "spark.sql.adaptive.coalescePartitions.enabled":"true", 
"spark.sql.adaptive.skewJoin.enabled":"true","spark.sql.shuffle.partitions": "12","spark.dynamicAllocation.enabled":"true","spark.shuffle.service.enabled":"true",
"spark.dynamicAllocation.minExecutors":"1","spark.dynamicAllocation.initialExecutors":"3","spark.dynamicAllocation.maxExecutors":"5","spark.submit.deployMode":"cluster"}'::json, 'spark', true),
('{"cod_abi": 1025, "cod_tabella": "READDR", "cod_provenienza": "CA"}'::jsonb, 'configurazione per READDR banca 1025 provenienza CA', 'gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/main_processor.py', '{gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/ingestion_process-1.0.0-py3-none-any.whl}', '{gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/jar/mssql-jdbc-12.2.0.jre11.jar,gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/jar/postgresql-42.7.8.jar}', NULL, NULL, NULL, '{ "spark.driver.cores" : "2","spark.driver.memory": "4g", "spark.executor.memory": "12g", "spark.executor.cores": "4",
 "spark.default.parallelism": "12","spark.ui.enabled" : "true", "spark.sql.adaptive.enabled": "true", "spark.sql.adaptive.coalescePartitions.enabled":"true", 
"spark.sql.adaptive.skewJoin.enabled":"true","spark.sql.shuffle.partitions": "12","spark.dynamicAllocation.enabled":"true","spark.shuffle.service.enabled":"true",
"spark.dynamicAllocation.minExecutors":"1","spark.dynamicAllocation.initialExecutors":"3","spark.dynamicAllocation.maxExecutors":"5","spark.submit.deployMode":"cluster"}'::json, 'spark', true),
('{"cod_abi": 1025, "cod_tabella": "READDR", "cod_provenienza": "N3"}'::jsonb, 'configurazione per READDR banca 1025 provenienza N3', 'gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/main_processor.py', '{gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/ingestion_process-1.0.0-py3-none-any.whl}', '{gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/jar/mssql-jdbc-12.2.0.jre11.jar,gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/jar/postgresql-42.7.8.jar}', NULL, NULL, NULL, '{ "spark.driver.cores" : "2","spark.driver.memory": "4g", "spark.executor.memory": "12g", "spark.executor.cores": "4",
 "spark.default.parallelism": "12","spark.ui.enabled" : "true", "spark.sql.adaptive.enabled": "true", "spark.sql.adaptive.coalescePartitions.enabled":"true", 
"spark.sql.adaptive.skewJoin.enabled":"true","spark.sql.shuffle.partitions": "12","spark.dynamicAllocation.enabled":"true","spark.shuffle.service.enabled":"true",
"spark.dynamicAllocation.minExecutors":"1","spark.dynamicAllocation.initialExecutors":"3","spark.dynamicAllocation.maxExecutors":"5","spark.submit.deployMode":"cluster"}'::json, 'spark', true),
('{"cod_abi": 1025, "cod_tabella": "READDR", "cod_provenienza": "TF"}'::jsonb, 'configurazione per READDR banca 1025 provenienza TF', 'gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/main_processor.py', '{gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/ingestion_process-1.0.0-py3-none-any.whl}', '{gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/jar/mssql-jdbc-12.2.0.jre11.jar,gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/jar/postgresql-42.7.8.jar}', NULL, NULL, NULL, '{ "spark.driver.cores" : "2","spark.driver.memory": "4g", "spark.executor.memory": "12g", "spark.executor.cores": "4",
 "spark.default.parallelism": "12","spark.ui.enabled" : "true", "spark.sql.adaptive.enabled": "true", "spark.sql.adaptive.coalescePartitions.enabled":"true", 
"spark.sql.adaptive.skewJoin.enabled":"true","spark.sql.shuffle.partitions": "12","spark.dynamicAllocation.enabled":"true","spark.shuffle.service.enabled":"true",
"spark.dynamicAllocation.minExecutors":"1","spark.dynamicAllocation.initialExecutors":"3","spark.dynamicAllocation.maxExecutors":"5","spark.submit.deployMode":"cluster"}'::json, 'spark', true),
('{}'::jsonb, 'configurazione di default', 'gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/main_processor.py', '{gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/ingestion_process-1.0.0-py3-none-any.whl}', '{gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/jar/mssql-jdbc-12.2.0.jre11.jar,gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/jar/postgresql-42.7.8.jar}', NULL, NULL, NULL, '{ "spark.driver.cores" : "1","spark.driver.memory": "2g", "spark.executor.memory": "12g", "spark.executor.cores": "4",
 "spark.default.parallelism": "12","spark.ui.enabled" : "true", "spark.sql.adaptive.enabled": "true", "spark.sql.adaptive.coalescePartitions.enabled":"true", 
"spark.sql.adaptive.skewJoin.enabled":"true","spark.sql.shuffle.partitions": "12","spark.dynamicAllocation.enabled":"true","spark.shuffle.service.enabled":"true",
"spark.dynamicAllocation.minExecutors":"1","spark.dynamicAllocation.initialExecutors":"3","spark.dynamicAllocation.maxExecutors":"5","spark.submit.deployMode":"cluster"}'::json, 'spark', false),
('{"cod_abi": 3296, "cod_tabella": "READDR", "cod_provenienza": "DP"}'::jsonb, 'configurazione per READDR banca 3296 provenienza DP', 'gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/main_processor.py', '{gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/ingestion_process-1.0.0-py3-none-any.whl}', '{gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/jar/mssql-jdbc-12.2.0.jre11.jar,gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/jar/postgresql-42.7.8.jar}', NULL, NULL, NULL, '{ "spark.driver.cores" : "2","spark.driver.memory": "4g", "spark.executor.memory": "12g", "spark.executor.cores": "4",
 "spark.default.parallelism": "12","spark.ui.enabled" : "true", "spark.sql.adaptive.enabled": "true", "spark.sql.adaptive.coalescePartitions.enabled":"true", 
"spark.sql.adaptive.skewJoin.enabled":"true","spark.sql.shuffle.partitions": "12","spark.dynamicAllocation.enabled":"true","spark.shuffle.service.enabled":"true",
"spark.dynamicAllocation.minExecutors":"1","spark.dynamicAllocation.initialExecutors":"3","spark.dynamicAllocation.maxExecutors":"5","spark.submit.deployMode":"cluster"}'::json, 'spark', true),
('{"cod_abi": 1025, "cod_tabella": "READDR", "cod_provenienza": "M2"}'::jsonb, 'configurazione per READDR banca 1025 provenienza M2', 'gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/main_processor.py', '{gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/ingestion_process-1.0.0-py3-none-any.whl}', '{gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/jar/mssql-jdbc-12.2.0.jre11.jar,gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/jar/postgresql-42.7.8.jar}', NULL, NULL, NULL, '{ "spark.driver.cores" : "2","spark.driver.memory": "4g", "spark.executor.memory": "12g", "spark.executor.cores": "4",
 "spark.default.parallelism": "12","spark.ui.enabled" : "true", "spark.sql.adaptive.enabled": "true", "spark.sql.adaptive.coalescePartitions.enabled":"true", 
"spark.sql.adaptive.skewJoin.enabled":"true","spark.sql.shuffle.partitions": "12","spark.dynamicAllocation.enabled":"true","spark.shuffle.service.enabled":"true",
"spark.dynamicAllocation.minExecutors":"1","spark.dynamicAllocation.initialExecutors":"3","spark.dynamicAllocation.maxExecutors":"5","spark.submit.deployMode":"cluster"}'::json, 'spark', true),
('{"cod_abi": 1025, "cod_tabella": "READDR", "cod_provenienza": "VR"}'::jsonb, 'configurazione per READDR banca 1025 provenienza VR', 'gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/main_processor.py', '{gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/ingestion_process-1.0.0-py3-none-any.whl}', '{gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/jar/mssql-jdbc-12.2.0.jre11.jar,gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/jar/postgresql-42.7.8.jar}', NULL, NULL, NULL, '{ "spark.driver.cores" : "2","spark.driver.memory": "4g", "spark.executor.memory": "12g", "spark.executor.cores": "4",
 "spark.default.parallelism": "12","spark.ui.enabled" : "true", "spark.sql.adaptive.enabled": "true", "spark.sql.adaptive.coalescePartitions.enabled":"true", 
"spark.sql.adaptive.skewJoin.enabled":"true","spark.sql.shuffle.partitions": "12","spark.dynamicAllocation.enabled":"true","spark.shuffle.service.enabled":"true",
"spark.dynamicAllocation.minExecutors":"1","spark.dynamicAllocation.initialExecutors":"3","spark.dynamicAllocation.maxExecutors":"5","spark.submit.deployMode":"cluster"}'::json, 'spark', true),
('{"cod_abi": 3296, "cod_tabella": "READDR", "cod_provenienza": "DN"}'::jsonb, 'configurazione per READDR banca 3296 provenienza DP', 'gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/main_processor.py', '{gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/ingestion_process-1.0.0-py3-none-any.whl}', '{gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/jar/mssql-jdbc-12.2.0.jre11.jar,gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/jar/postgresql-42.7.8.jar}', NULL, NULL, NULL, '{ "spark.driver.cores" : "2","spark.driver.memory": "4g", "spark.executor.memory": "12g", "spark.executor.cores": "4",
 "spark.default.parallelism": "12","spark.ui.enabled" : "true", "spark.sql.adaptive.enabled": "true", "spark.sql.adaptive.coalescePartitions.enabled":"true", 
"spark.sql.adaptive.skewJoin.enabled":"true","spark.sql.shuffle.partitions": "12","spark.dynamicAllocation.enabled":"true","spark.shuffle.service.enabled":"true",
"spark.dynamicAllocation.minExecutors":"1","spark.dynamicAllocation.initialExecutors":"3","spark.dynamicAllocation.maxExecutors":"5","spark.submit.deployMode":"cluster"}'::json, 'spark', true)

DROP TABLE if exists public.tab_registro_mensile;

CREATE TABLE public.tab_registro_mensile (
    chiave JSONB PRIMARY KEY,
    last_id BIGINT NOT NULL,
    max_data_va INT4,
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

GRANT SELECT,insert,update,delete,truncate ON table public.tab_semaforo_ready TO nplg_app; 

drop table if exists public.tab_semaforo_domini;

CREATE TABLE public.tab_semaforo_domini (
	id int4 NOT NULL,
	cod_abi int4 NULL,
	periodo_rif int4 NULL,
	tabella varchar(128) NOT NULL,
	tipo_caricamento varchar(8) NOT NULL,
	provenienza bpchar(2) NOT NULL,
	id_file varchar(32) NULL,
	colonna_valore varchar(16) NULL,
	ambito int4 NULL,
	o_carico timestamp(3) NULL,
	CONSTRAINT pk_tab_semaforo_domini PRIMARY KEY (id)
);


GRANT TRUNCATE, INSERT, SELECT ON public.tab_semaforo_domini TO nplg_app;


drop table if exists public.tab_no_semaforo;

CREATE TABLE public.tab_no_semaforo (
	tabella varchar(128) NOT NULL,
    tipo_caricamento varchar(8) NOT NULL DEFAULT 'NO_SEM',
	CONSTRAINT pk_tab_no_semaforo PRIMARY KEY (tabella)
);


GRANT TRUNCATE, INSERT, select ON public.tab_no_semaforo TO nplg_app;

DROP TABLE if exists public.tab_semaforo_ready;

CREATE TABLE public.tab_semaforo_ready (
	uid uuid NULL,
	source_id varchar(128) NULL,
	destination_id varchar(128) NULL,
	tipo_caricamento varchar NULL,
	"key" jsonb NULL,
	query_param jsonb null,
	is_heavy bool
);
GRANT SELECT,DELETE,UPDATE,TRUNCATE ON table public.tab_semaforo_ready TO nplg_app;

insert into public.tab_semaforo_ready
select tb1.*, coalesce(is_heavy, false)  from
(
	(select
	gen_random_uuid() as uid,s.tabella as source_id,s.tabella as destination_id,s.tipo_caricamento,s.key,
	case
			when tipo_caricamento in ('TABUT','Tabut','Etraz','ESTRAZ', 'DOMINI', 'Domini') then
            	jsonb_build_object('id',id)
			else
            	jsonb_build_object('id',id,
            					   'cod_abi',cod_abi,
								   'cod_provenienza',provenienza,
								   'num_periodo_rif',periodo_rif,
								   'cod_colonna_valore',colonna_valore,
								   'num_ambito',ambito,
									'max_data_va',
        CASE
            WHEN tipo_caricamento IN ('CLIENTI', 'Clienti')
                THEN COALESCE(max_data_va, 20000101)
            ELSE
                COALESCE(max_data_va, 20000101000000000)
        END
            )
		end as query_param from
(select
		id,
		tabella,
		tipo_caricamento ,
		colonna_valore,
		ambito,cod_abi,provenienza,periodo_rif,
		case
			when tipo_caricamento in ('TABUT','Tabut','Estraz','ESTRAZ') then jsonb_build_object('cod_tabella',tabella)
		else jsonb_build_object('cod_abi',cod_abi,'cod_tabella',tabella,'cod_provenienza',provenienza)
		end as key
	from
		public.tab_semaforo_mensile
		) s
		left join public.tab_registro_mensile r
    on
	r.chiave = s.key
where
	r.last_id is null
	or s.id > r.last_id
) union all
(
select
	gen_random_uuid() as uid,s.tabella as source_id,s.tabella as destination_id,s.tipo_caricamento,s.key,
	jsonb_build_object('id',id) 	as query_param from
(select
		id,
		tabella,
		tipo_caricamento ,
		colonna_valore,
		ambito,cod_abi,provenienza, periodo_rif,
		jsonb_build_object('cod_tabella',tabella) as key
	from
		public.tab_semaforo_domini) s
		left join public.tab_registro_mensile r
    on
	r.chiave = s.key
where
	r.last_id is null
	or s.id > r.last_id
) union all
(select  gen_random_uuid() as uid,
		tabella as source_id,tabella as destination_id,
		null as tipo_caricamento, null as key, null as query_param
		from public.tab_no_semaforo
)) tb1 left join 
public.tab_task_configs
on tb1.key = tab_task_configs.key;

drop table if exists public.tab_task_sources;

CREATE TABLE public.tab_task_sources (
	id varchar NOT NULL,
    source_id varchar NOT null UNIQUE,
	source_type varchar NOT NULL,
	CONSTRAINT chk_source_type CHECK (((source_type)::text = ANY ((ARRAY['jdbc'::character varying, 'bigquery'::character varying, 'file'::character varying, 'JDBC'::character varying, 'BIGQUERY'::character varying, 'FILE'::character varying])::text[]))),
	CONSTRAINT tab_task_sources_pk PRIMARY KEY (source_id)
);

GRANT SELECT ON table public.tab_task_sources TO nplg_app;

INSERT INTO public.tab_task_sources
(id, source_id, source_type)
VALUES
('REAGDG', 'REAGDG', 'JDBC'),
('READDR', 'READDR', 'JDBC');

drop table if exists public.tab_task_destinations;

CREATE TABLE public.tab_task_destinations (
	id varchar NOT NULL,
    destination_id varchar NOT null UNIQUE,
	destination_type varchar NOT NULL,
	CONSTRAINT chk_destination_type CHECK (((destination_type)::text = ANY ((ARRAY['jdbc'::character varying, 'bigquery'::character varying, 'file'::character varying, 'JDBC'::character varying, 'BIGQUERY'::character varying, 'FILE'::character varying])::text[]))),
	CONSTRAINT tab_task_destinations_pk PRIMARY KEY (destination_id)
);


GRANT SELECT ON table public.tab_task_destinations TO nplg_app;

INSERT INTO public.tab_task_destinations
(id, destination_id, destination_type)
VALUES
('REAGDG_STG', 'REAGDG_STG', 'file'),
('READDR_STG', 'READDR_STG', 'file')
;



drop table if exists public.tab_jdbc_sources;

CREATE TABLE public.tab_jdbc_sources (
	source_id varchar NOT NULL,
	url varchar NOT NULL,
	username varchar NOT NULL,
	pwd varchar NOT NULL,
	driver varchar NOT NULL,
	tablename varchar NULL,
	query_text varchar NULL,
	CONSTRAINT check_one_column_populated CHECK ((((tablename IS NOT NULL) AND (query_text IS NULL)) OR ((tablename IS NULL) AND (query_text IS NOT NULL)))),
	CONSTRAINT tab_jdbc_sources_pk PRIMARY KEY (source_id)
);

ALTER TABLE public.tab_jdbc_sources ADD CONSTRAINT fk_tab_jdbc_sources FOREIGN KEY (source_id) REFERENCES public.tab_task_sources(source_id) ON DELETE CASCADE ON UPDATE CASCADE;

GRANT SELECT ON table public.tab_jdbc_sources TO nplg_app;

INSERT INTO public.tab_jdbc_sources
(source_id, url, username, pwd, driver, tablename, query_text)
VALUES
('REAGDG', 'jdbc:sqlserver://pdbclt076.syssede.systest.sanpaoloimi.com\\\\SYD202:1433;DatabaseName=RDBP0_MENS;encrypt=true;trustServerCertificate=true;integratedSecurity=true;authenticationScheme=NTLM;', 'SYS_LG_RDB@SYSSPIMI', 'SECRET::SYS_LG_RDB@SYSSPIMI', 'com.microsoft.sqlserver.jdbc.SQLServerDriver', NULL,
$$SELECT BANCA as NUM_BANCA,LTRIM(RTRIM(NDG)) as COD_NDG,LTRIM(RTRIM(INTESTAZIONE)) as COD_INTESTAZIONE,LTRIM(RTRIM(CODICE_FISCALE)) as COD_FISCALE,LTRIM(RTRIM(COD_TIP_NDG)) as COD_TIP_NDG,LTRIM(RTRIM(COD_PIVA)) as COD_PIVA,LTRIM(RTRIM(COD_STATO_NDG)) as COD_STATO_NDG,LTRIM(RTRIM(COD_SPECIE_GIURIDICA)) as COD_SPECIE_GIURIDICA, LTRIM(RTRIM(COD_UO_CAPOFILA)) as COD_UO_CAPOFILA, LTRIM(RTRIM(DATA_NASC_COST)) as COD_DATA_NASC_COST,LTRIM(RTRIM(COD_COMUNE_NASCITA)) as COD_COMUNE_NASCITA, LTRIM(RTRIM(COD_PROVNC_NASCT_COST)) as COD_PROVNC_NASCT_COST,LTRIM(RTRIM(COD_CAP_RESIDENZA)) as COD_CAP_RESIDENZA, LTRIM(RTRIM(DES_COMUNE_RESIDENZA)) as DES_COMUNE_RESIDENZA,LTRIM(RTRIM(COD_PROVINCIA_RESIDENZA)) as COD_PROVINCIA_RESIDENZA, LTRIM(RTRIM(COD_SOTTOSEGMENTO_ECONOMICO)) as COD_SOTTOSEGMENTO_ECONOMICO,LTRIM(RTRIM(COD_SOTTOSEGMENTO_COMMERCIALE)) as COD_SOTTOSEGMENTO_COMMERCIALE, LTRIM(RTRIM(COD_CLI_NOPROFIT)) as COD_CLI_NOPROFIT,LTRIM(RTRIM(COD_SEGMENTO_ECONOMICO)) as COD_SEGMENTO_ECONOMICO, LTRIM(RTRIM(COD_PTF_CDG)) as COD_PTF_CDG, LTRIM(RTRIM(COD_UTENZA_MODULO)) as COD_UTENZA_MODULO,LTRIM(RTRIM(COD_SEGM_CDG)) as COD_SEGM_CDG, LTRIM(RTRIM(COD_DESK_CLIENTE)) as COD_DESK_CLIENTE, LTRIM(RTRIM(CLASSE_SOA_UO_PTF)) as COD_CLASSE_SOA_UO_PTF,LTRIM(RTRIM(COD_MODULO_CDG)) as COD_MODULO_CDG, LTRIM(RTRIM(COD_GESTORE_REMOTO)) as COD_GESTORE_REMOTO, LTRIM(RTRIM(RIF_REGOLA_DESK)) as COD_RIF_REGOLA_DESK,LTRIM(RTRIM(COD_BU_TERRTRL)) as COD_BU_TERRTRL, LTRIM(RTRIM(COD_TIP_GESTR_GRM)) as COD_TIP_GESTR_GRM, LTRIM(RTRIM(COD_ATECO)) as COD_ATECO,LTRIM(RTRIM(COD_TIP_GESTR_INDUSTRY)) as COD_TIP_GESTR_INDUSTRY, LTRIM(RTRIM(COD_RAE)) as COD_RAE, LTRIM(RTRIM(COD_BU)) as COD_BU, LTRIM(RTRIM(COD_SAE)) as COD_SAE,LTRIM(RTRIM(COD_GRM)) as COD_GRM, LTRIM(RTRIM(COD_SESSO)) as COD_SESSO, LTRIM(RTRIM(COD_UTENZA_GRM)) as COD_UTENZA_GRM,LTRIM(RTRIM(COD_CAPO_INDUSTRY)) as COD_CAPO_INDUSTRY, LTRIM(RTRIM(COD_NDG_PREVLNT)) as COD_NDG_PREVLNT, LTRIM(RTRIM(COD_ABI_SNDG_NDG_PREV)) as COD_ABI_SNDG_NDG_PREV,LTRIM(RTRIM(COD_SNDG_NDG_PREV)) as COD_SNDG_NDG_PREV, LTRIM(RTRIM(FLG_RESIDENTE)) as COD_FLG_RESIDENTE, LTRIM(RTRIM(COD_RILVNZ_ARTCL_136_TUB)) as COD_RILVNZ_ARTCL_136_TUB,LTRIM(RTRIM(COD_RILVNZ_ARTCL_53_TUB)) as COD_RILVNZ_ARTCL_53_TUB, LTRIM(RTRIM(COD_RILVNZ_CONSOB)) as COD_RILVNZ_CONSOB, LTRIM(RTRIM(COD_RILVNZ_IAS_24)) as COD_RILVNZ_IAS_24,LTRIM(RTRIM(COD_NDG_NO_PUSP)) as COD_NDG_NO_PUSP, LTRIM(RTRIM(COD_TIP_GESTN)) as COD_TIP_GESTN, LTRIM(RTRIM(FLG_CAPO_FILR)) as COD_FLG_CAPO_FILR,LTRIM(RTRIM(FLG_APPRTNZ_FILR)) as COD_FLG_APPRTNZ_FILR, LTRIM(RTRIM(COD_SAG)) as COD_SAG, LTRIM(RTRIM(MACRO_CLUSTER_RATING)) as COD_MACRO_CLUSTER_RATING,LTRIM(RTRIM(CLUSTER_RATING)) as COD_CLUSTER_RATING, LTRIM(RTRIM(CLASSE_UNICA_RATING)) as COD_CLASSE_UNICA_RATING, LTRIM(RTRIM(PD_MEDIA)) as COD_PD_MEDIA,LTRIM(RTRIM(PROV_RATING)) as COD_PROV_RATING, LTRIM(RTRIM(COD_COLORE_CLASSE_CRA)) as COD_COLORE_CLASSE_CRA,LTRIM(RTRIM(COD_CLASSE_RATING_PIU_CRA)) as COD_CLASSE_RATING_PIU_CRA, LTRIM(RTRIM(COD_CDN)) as COD_CDN,LTRIM(RTRIM(SEGMENTO_REGOLAMENTARE)) as COD_SEGMENTO_REGOLAMENTARE, LTRIM(RTRIM(SEGMENTO_COMPORTAMENTALE)) as COD_SEGMENTO_COMPORTAMENTALE,LTRIM(RTRIM(FLG_CONDIVISO)) as COD_FLG_CONDIVISO,case when convert(numeric, data_va) <=20000000 then 20010101 else coalesce(convert(numeric, data_va),20010101) end as NUM_DATA_VA,DATA_INS as NUM_DATA_INS,PERIODO_RIF as NUM_PERIODO_RIF, ${num_periodo_rif} as NUM_PERIODO_COMP FROM REAGDG with (nolock) where BANCA=${cod_abi} and case when convert(int, data_va) <=20000000 then 20010101 else coalesce(convert(int, data_va),20010101) end >=${max_data_va}$$
),
('READDR', 'jdbc:sqlserver://pdbclt076.syssede.systest.sanpaoloimi.com\\\\SYD202:1433;DatabaseName=RDBP0_MENS;encrypt=true;trustServerCertificate=true;integratedSecurity=true;authenticationScheme=NTLM;', 'SYS_LG_RDB@SYSSPIMI', 'SECRET::SYS_LG_RDB@SYSSPIMI', 'com.microsoft.sqlserver.jdbc.SQLServerDriver', NULL,
$$SELECT BANCA as NUM_BANCA, LTRIM(RTRIM(COD_UO)) as COD_UO,LTRIM(RTRIM(NUM_PARTITA)) as COD_NUM_PARTITA,COD_PROVN_DATI_RED,LTRIM(RTRIM(FORMA_TECNICA)) as COD_FORMA_TECNICA,LTRIM(RTRIM(COD_DIVISA)) as COD_DIVISA,LTRIM(RTRIM(NDG)) as COD_NDG,LTRIM(RTRIM(PTF_RAPPORTO)) as COD_PTF_RAPPORTO, LTRIM(RTRIM(ID_MIGRZ_KTO)) as COD_ID_MIGRZ_KTO,LTRIM(RTRIM(CANALE_VENDITA)) as COD_CANALE_VENDITA,LTRIM(RTRIM(SUB_ID_MIGR_RAPPORTO)) as COD_SUB_ID_MIGR_RAPPORTO,LTRIM(RTRIM(UO_PTF_RAPPORTO)) as COD_UO_PTF_RAPPORTO,LTRIM(RTRIM(COD_STATO_KTO)) as COD_STATO_KTO,LTRIM(RTRIM(COD_PRODOTTO_LEGACY)) as COD_PRODOTTO_LEGACY,LTRIM(RTRIM(COD_PRODOTTO_RAPPORTO)) as COD_PRODOTTO_RAPPORTO,LTRIM(RTRIM(DESK_RAPPORTO)) as COD_DESK_RAPPORTO,LTRIM(RTRIM(COD_RAPP_ANAG)) as COD_RAPP_ANAG,LTRIM(RTRIM(COD_TIP_TASSO)) as COD_TIP_TASSO,COALESCE(TRY_CONVERT(int, NULLIF(LTRIM(RTRIM(DATA_ACCENSIONE)), 'NA')),0) AS NUM_DATA_ACCENSIONE,COALESCE(TRY_CONVERT(int, NULLIF(LTRIM(RTRIM(DATA_ESTINZIONE)), 'NA')),0) AS NUM_DATA_ESTINZIONE,COALESCE(TRY_CONVERT(int, NULLIF(LTRIM(RTRIM(DATA_SCADENZA)), 'NA')),0) AS NUM_DATA_SCADENZA,LTRIM(RTRIM(PTF_SPECCHIO)) as COD_PTF_SPECCHIO,LTRIM(RTRIM(COD_CANALE_PROPOSTA)) as COD_CANALE_PROPOSTA,LTRIM(RTRIM(PRIORITA_DESK_RAPPORTO)) as COD_PRIORITA_DESK_RAPPORTO,LTRIM(RTRIM(DESK_RENDICONTATIVO_RAPPORTO)) as COD_DESK_RENDICONTATIVO_RAPPORTO,LTRIM(RTRIM(PRODOTTO_COMMERCIALE_DINAMICO)) as COD_PRODOTTO_COMMERCIALE_DINAMICO,LTRIM(RTRIM(COD_DIVISIONE)) as COD_DIVISIONE,LTRIM(RTRIM(COD_DIVISIONE_COMMERCIALE)) as COD_DIVISIONE_COMMERCIALE,cast(format(DATAORA_UPD,'yyyyMMddHHmmssfff') as decimal) as NUM_DATA_VA,DATA_INS as NUM_DATA_INS,case when convert(int, periodo_rif) <=200000 then 200101 else coalesce(convert(int, periodo_rif),200101) end as NUM_PERIODO_RIF, {periodo_comp} as NUM_PERIODO_COMP, row_number() over(order by cod_uo, num_partita) as ROW_N from READDR with (nolock) where BANCA={banca_in_elaborazione} and COD_PROVN_DATI_RED = '{provenienza_in_elaborazione}' and DATAORA_UPD >convert(datetime, stuff(stuff(stuff(stuff(cast({max_data_va} as decimal), 9, 0, ' '), 12, 0, ':'), 15, 0, ':'),18,0,'.'))$$)
;


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
('READDR_STG', 'parquet', 'gs://bkt-isp-nplg0-dpstg-svil-001-ew12/staging/READDR_STG', false, NULL);


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

INSERT INTO public.tab_bigquery_destinations
(destination_id, project, dataset, tablename, gcs_bucket, use_direct_write, "columns", overwrite)
VALUES('REAGDG_STG', 'prj-isp-nplg0-appl-svil-001', 'NPLG0W', 'TB_REAGDG_SG', NULL, true, '["NUM_BANCA", "COD_NDG", "COD_INTESTAZIONE", "COD_FISCALE", "COD_TIP_NDG", "COD_PIVA", "COD_STATO_NDG", "COD_SPECIE_GIURIDICA", "COD_UO_CAPOFILA", "COD_DATA_NASC_COST", "COD_COMUNE_NASCITA", "COD_PROVNC_NASCT_COST", "COD_CAP_RESIDENZA", "DES_COMUNE_RESIDENZA", "COD_PROVINCIA_RESIDENZA", "COD_SOTTOSEGMENTO_ECONOMICO", "COD_SOTTOSEGMENTO_COMMERCIALE", "COD_CLI_NOPROFIT", "COD_SEGMENTO_ECONOMICO", "COD_PTF_CDG", "COD_UTENZA_MODULO", "COD_SEGM_CDG", "COD_DESK_CLIENTE", "COD_CLASSE_SOA_UO_PTF", "COD_MODULO_CDG", "COD_GESTORE_REMOTO", "COD_RIF_REGOLA_DESK", "COD_BU_TERRTRL", "COD_TIP_GESTR_GRM", "COD_ATECO", "COD_TIP_GESTR_INDUSTRY", "COD_RAE", "COD_BU", "COD_SAE", "COD_GRM", "COD_SESSO", "COD_UTENZA_GRM", "COD_CAPO_INDUSTRY", "COD_NDG_PREVLNT", "COD_ABI_SNDG_NDG_PREV", "COD_SNDG_NDG_PREV", "COD_FLG_RESIDENTE", "COD_RILVNZ_ARTCL_136_TUB", "COD_RILVNZ_ARTCL_53_TUB", "COD_RILVNZ_CONSOB", "COD_RILVNZ_IAS_24", "COD_NDG_NO_PUSP", "COD_TIP_GESTN", "COD_FLG_CAPO_FILR", "COD_FLG_APPRTNZ_FILR", "COD_SAG", "COD_MACRO_CLUSTER_RATING", "COD_CLUSTER_RATING", "COD_CLASSE_UNICA_RATING", "COD_PD_MEDIA", "COD_PROV_RATING", "COD_COLORE_CLASSE_CRA", "COD_CLASSE_RATING_PIU_CRA", "COD_CDN", "COD_SEGMENTO_REGOLAMENTARE", "COD_SEGMENTO_COMPORTAMENTALE", "COD_FLG_CONDIVISO", "NUM_DATA_VA", "NUM_DATA_INS", "NUM_PERIODO_RIF", "NUM_PERIODO_COMP"]'::jsonb, false);


DROP table if exists public.tab_task_logs;

CREATE TABLE public.tab_task_logs (
	"key" jsonb NOT NULL,
	periodo int4 NULL,
	run_id varchar NOT NULL,
	state_id varchar NOT NULL,
	description varchar NULL,
	error_message varchar NULL,
	update_ts timestamp DEFAULT now() NOT NULL,
	rows_affected int4 NULL
);

GRANT SELECT, INSERT, UPDATE ON table public.tab_task_logs TO nplg_app;

DROP TABLE if exists public.tab_configurations;

CREATE TABLE public.tab_configurations (
	config_name varchar NOT NULL,
	config_value varchar NOT NULL,
	description varchar NULL,
	CONSTRAINT tab_configurations_pk PRIMARY KEY (config_name)
);

GRANT SELECT ON table public.tab_configurations TO nplg_app;

INSERT INTO public.tab_configurations
(config_name, config_value, description)
VALUES('project', 'prj-isp-nplg0-appl-test-001', 'Dataproc GCP project');
INSERT INTO public.tab_configurations
(config_name, config_value, description)
VALUES('region', 'europe-west12', 'Dataproc region');
INSERT INTO public.tab_configurations
(config_name, config_value, description)
VALUES('cluster_name', 'dprcpclt-isp-nplg0-test-ew12-01', 'Dataproc cluster name used for job placement');
INSERT INTO public.tab_configurations
(config_name, config_value, description)
VALUES('environment', 'test', 'Environment (test, test or prod)');
INSERT INTO public.tab_configurations
(config_name, config_value, description)
VALUES('poll_sleep_time_seconds', '60','Poll time to wait for the completion of a job (seconds)');
INSERT INTO public.tab_configurations
(config_name, config_value, description)
VALUES('ingestion_max_contemporary_tasks','10','max contemporary tasks');
