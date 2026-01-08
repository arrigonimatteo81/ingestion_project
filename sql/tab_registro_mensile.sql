CREATE TABLE public.tab_registro_mensile (
    chiave JSONB PRIMARY KEY,
    last_id BIGINT NOT NULL,
    max_data_va INT4,
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

GRANT SELECT,insert,update,delete,truncate ON table public.tab_semaforo_ready TO fdir_app; --utente




