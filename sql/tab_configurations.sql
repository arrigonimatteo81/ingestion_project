DROP TABLE if exists public.tab_configurations;

CREATE TABLE public.tab_configurations (
	config_name varchar NOT NULL,
	config_value varchar NOT NULL,
	description varchar NULL,
	CONSTRAINT tab_configurations_pk PRIMARY KEY (config_name)
);