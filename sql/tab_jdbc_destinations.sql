drop table if exists public.tab_jdbc_destinations;

CREATE TABLE public.tab_jdbc_destinations (
	destination_id varchar NOT NULL,
	url varchar NOT NULL,
	username varchar NOT NULL,
	pwd varchar NOT NULL,
	driver varchar NOT NULL,
	tablename varchar NOT NULL,
	CONSTRAINT tab_jdbc_destinations_pk PRIMARY KEY (destination_id)
);

ALTER TABLE public.tab_jdbc_destinations ADD CONSTRAINT fk_tab_jdbc_destinations FOREIGN KEY (destination_id) REFERENCES public.tab_task_destinations(destination_id) ON DELETE CASCADE ON UPDATE CASCADE;