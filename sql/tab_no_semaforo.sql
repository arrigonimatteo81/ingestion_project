drop table if exists public.tab_no_semaforo;

CREATE TABLE public.tab_no_semaforo (
	tabella varchar(128) NOT NULL,
    tipo_caricamento varchar(8) NOT NULL DEFAULT 'NO_SEM',
	CONSTRAINT pk_tab_no_semaforo PRIMARY KEY (tabella)
);


GRANT TRUNCATE, INSERT, select ON public.tab_no_semaforo TO nplg_app;