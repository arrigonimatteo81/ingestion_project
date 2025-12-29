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
and sm.tabella = rm.tabella
and sm.provenienza = rm.provenienza
where sm.id>coalesce(rm.id,0)