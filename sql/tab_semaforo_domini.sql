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


GRANT TRUNCATE, INSERT, SELECT ON public.tab_semaforo_domini TO fdir_app;