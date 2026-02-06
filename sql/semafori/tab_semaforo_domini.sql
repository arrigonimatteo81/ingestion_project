drop table if exists public.tab_semaforo_domini;

CREATE TABLE public.tab_semaforo_domini (
	id int4 NOT NULL,
	tipo_caricamento varchar NOT NULL,
	tabella varchar(128) NOT NULL
	CONSTRAINT pk_tab_semaforo_domini PRIMARY KEY (id)
);


GRANT TRUNCATE, INSERT, SELECT ON public.tab_semaforo_domini TO nplg_app;