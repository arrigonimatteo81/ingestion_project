select
coalesce(rm.id,0), sm.id,
coalesce(rm.cod_abi,sm.cod_abi) as cod_abi,
coalesce(rm.tabella, sm.tabella) as tabella,
coalesce(rm.provenienza, sm.provenienza) as provenienza,
sm.periodo_rif,
coalesce(rm.tipo_caricamento, sm.tipo_caricamento) as tipo_caricamento,
coalesce(rm.colonna_valore, sm.colonna_valore) as colonna_valore,
coalesce(rm.ambito, sm.ambito) as ambito,
coalesce(rm.max_datava, 20000101000000) as max_datava
from public.semaforo_mensile sm
left join public.registro_mensile rm
on sm.cod_abi = rm.cod_abi
and sm.tabella = rm.tabella
and sm.provenienza = rm.provenienza