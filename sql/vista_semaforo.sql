CREATE TABLE public.tab_registro_mensile (
    chiave JSONB PRIMARY KEY,
    last_id BIGINT NOT NULL,
    max_data_va INT4,
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE public.semaforo_mensile (
	id int4 PRIMARY KEY,
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
	o_carico_sys timestamp(3) NULL
);

create view public.semaforo_ready as
select
	gen_random_uuid() as uid,s.tabella as source_id,s.tabella as destination_id,s.tipo_caricamento,s.key,
	case
			when tipo_caricamento = 'DOMINI' then
            	jsonb_build_object('id',id)
			else
            	jsonb_build_object('id',id,
            					   'cod_abi',cod_abi,
								   'cod_provenienza',provenienza,
								   'num_periodo_rif',periodo_rif,
								   'cod_colonna_valore',colonna_valore,
								   'num_ambito',ambito,
								   'max_data_va', coalesce(max_data_va,20000101)
            )
		end as query_param
from
	(
	select
		id,
		tabella,
		tipo_caricamento ,
		colonna_valore,
		ambito,cod_abi,provenienza,periodo_rif,
		case
			when tipo_caricamento = 'DOMINI' then
            	jsonb_build_object('tabella',tabella)
			else
            	jsonb_build_object('cod_abi',cod_abi,
								   'tabella',tabella,
								   'provenienza',provenienza
            )
		end as key
	from
		public.semaforo_mensile)s
left join public.tab_registro_mensile r
    on
	r.chiave = s.key
where
	r.last_id is null
	or s.id > r.last_id;

GRANT SELECT ON TABLE public.semaforo_ready TO fdir_app;





