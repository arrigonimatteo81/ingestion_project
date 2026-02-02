drop table if exists public.tab_semaforo_mensile;

CREATE TABLE public.tab_semaforo_mensile (
	id int4 NOT NULL,
	cod_abi int4 NULL,
	periodo_rif int4 NULL,
	tabella varchar(128) NOT NULL,
	tipo_caricamento varchar NOT NULL,
	provenienza bpchar(2) NOT NULL,
	id_file varchar(32) NULL,
	colonna_valore varchar(16) NULL,
	ambito int4 NULL,
	o_carico timestamp(3) NULL,
	CONSTRAINT pk_tab_semaforo_mensile PRIMARY KEY (id)
);


GRANT TRUNCATE, INSERT, SELECT ON public.tab_semaforo_mensile TO nplg_app;


INSERT INTO public.tab_semaforo_mensile
(id, cod_abi, periodo_rif, tabella, tipo_caricamento, provenienza, id_file, colonna_valore, ambito, o_carico, stato, utente, o_carico_sys)
VALUES
(1, 1025, 202601, 'REAGDG', 'Clienti', 'AN', '', '', 0, CURRENT_TIMESTAMP, 0, 'MATTEO', CURRENT_TIMESTAMP), --heavy
(2, 3239, 202601, 'REAGDG', 'Clienti', 'AN', '', '', 0, CURRENT_TIMESTAMP, 0, 'MATTEO', CURRENT_TIMESTAMP),
(3, 3296, 202601, 'REAGDG', 'Clienti', 'AN', '', '', 0, CURRENT_TIMESTAMP, 0, 'MATTEO', CURRENT_TIMESTAMP),
(4, 3385, 202601, 'REAGDG', 'Clienti', 'AN', '', '', 0, CURRENT_TIMESTAMP, 0, 'MATTEO', CURRENT_TIMESTAMP),
(5, 32334, 202601, 'REAGDG', 'Clienti', 'AN', '', '', 0, CURRENT_TIMESTAMP, 0, 'MATTEO', CURRENT_TIMESTAMP),
(6, 1025, 202601, 'READDR', 'Rapporti', 'N1', '', '', 0, CURRENT_TIMESTAMP, 0, 'MATTEO', CURRENT_TIMESTAMP), --heavy
(7, 1025, 202601, 'READDR', 'Rapporti', 'M1', '', '', 0, CURRENT_TIMESTAMP, 0, 'MATTEO', CURRENT_TIMESTAMP),
(8, 3296, 202601, 'READDR', 'Rapporti', 'DP', '', '', 0, CURRENT_TIMESTAMP, 0, 'MATTEO', CURRENT_TIMESTAMP), --heavy
(9, 3385, 202601, 'READDR', 'Rapporti', 'VR', '', '', 0, CURRENT_TIMESTAMP, 0, 'MATTEO', CURRENT_TIMESTAMP),
(10, 32334, 202601, 'READDR', 'Rapporti', 'P0', '', '', 0, CURRENT_TIMESTAMP, 0, 'MATTEO', CURRENT_TIMESTAMP)
;