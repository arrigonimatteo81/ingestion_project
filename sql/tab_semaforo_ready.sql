DROP TABLE if exists public.tab_semaforo_ready;

CREATE TABLE public.tab_semaforo_ready (
	uid uuid NULL,
	source_id varchar(128) NULL,
	destination_id varchar(128) NULL,
	tipo_caricamento varchar NULL,
	"key" jsonb NULL,
	query_param jsonb NULL
);
GRANT SELECT,DELETE,UPDATE,TRUNCATE ON table public.tab_semaforo_ready TO fdir_app;

insert into public.semaforo_ready
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
on tb1.key = tab_task_configs.key