DROP TABLE if exists public.tab_configurations;

CREATE TABLE public.tab_configurations (
	config_name varchar NOT NULL,
	config_value varchar NOT NULL,
	description varchar NULL,
	CONSTRAINT tab_configurations_pk PRIMARY KEY (config_name)
);

GRANT SELECT ON table public.tab_configurations TO nplg_app;

INSERT INTO public.tab_configurations (config_name,config_value,description) VALUES
	 ('region','europe-west12','Dataproc region'),
	 ('poll_sleep_time_seconds','60','Poll time to wait for the completion of a job (seconds)'),
	 ('cluster_name','dprcpclt-isp-nplg0-svil-ew12-01','Dataproc cluster name used for job placement'),
	 ('environment','svil','Environment (svil, test or prod)'),
	 ('project','prj-isp-nplg0-appl-svil-001','Dataproc GCP project'),
	 ('ingestion_max_contemporary_tasks','8','max contemporary tasks in ingestion'),
	 ('silver_max_contemporary_tasks','100','max contemporary tasks in silver'),
	 ('bucket','bkt-isp-nplg0-dptmp-svil-001-ew12','GCP bucket temporaneo');
