CREATE TABLE public.registro_mensile (
	id_sem int4 NOT NULL,
	cod_abi int4 NULL,
	cod_tabella varchar(128) NOT NULL,
	cod_provenienza bpchar(2) NULL,
	max_datava bigint NULL,
	tms_ultima_modifica timestamp NULL,
	CONSTRAINT pk_registro_mensile PRIMARY KEY (id_sem)
);

CREATE TABLE public.semaforo_mensile (
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
	stato int4 NULL,
	utente varchar(255) NULL,
	o_carico_sys timestamp(3) NULL,
	CONSTRAINT pk_semaforo_mensile PRIMARY KEY (id)
);

insert into public.tab_tasks_semaforo(
    id,
    cod_abi,
    source_id,
    destination_id,
    cod_provenienza,
    num_periodo_rif,
    cod_gruppo,
    cod_colonna_valore,
    num_ambito,
    num_max_data_va
)
select
sm.id id_sem,
sm.cod_abi as cod_abi,
sm.tabella as cod_tabella,
sm.provenienza as cod_provenienza,
sm.periodo_rif as num_periodo_rif,
sm.tipo_caricamento as cod_tipo_caricamento,
sm.colonna_valore as cod_colonna_valore,
sm.ambito as num_ambito,
coalesce(rm.max_datava, 20000101000000) as num_max_datava
from public.semaforo_mensile sm
left join public.registro_mensile rm
on sm.cod_abi = rm.cod_abi
and sm.tabella = rm.cod_tabella
and sm.provenienza = rm.cod_provenienza
where sm.id>coalesce(rm.id_sem,0)