DROP TABLE if exists public.tab_configurations;

CREATE TABLE public.tab_configurations (
	config_name varchar NOT NULL,
	config_value varchar NOT NULL,
	description varchar NULL,
	CONSTRAINT tab_configurations_pk PRIMARY KEY (config_name)
);

GRANT SELECT ON table public.tab_configurations TO nplg_app;

INSERT INTO public.configurations
(config_name, config_value, description)
VALUES('project', 'prj-isp-nplg0-appl-test-001', 'Dataproc GCP project');
INSERT INTO public.configurations
(config_name, config_value, description)
VALUES('region', 'europe-west12', 'Dataproc region');
INSERT INTO public.configurations
(config_name, config_value, description)
VALUES('cluster_name', 'dprcpclt-isp-nplg0-test-ew12-01', 'Dataproc cluster name used for job placement');
INSERT INTO public.configurations
(config_name, config_value, description)
VALUES('environment', 'test', 'Environment (test, test or prod)');
INSERT INTO public.configurations
(config_name, config_value, description)
VALUES('poll_sleep_time_seconds', '60','Poll time to wait for the completion of a job (seconds)');