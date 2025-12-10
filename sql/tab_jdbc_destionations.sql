CREATE TABLE public.tab_destinations_jdbc (
	id_destination varchar NOT NULL,
	url varchar NOT NULL,
	username varchar NOT NULL,
	pwd varchar NOT NULL,
	driver varchar NOT NULL,
	tablename varchar NOT NULL,
	CONSTRAINT tab_destinations_jdbc_pk PRIMARY KEY (id_destination)
);


-- public.task_destinations_jdbc foreign keys

--ALTER TABLE public.task_destinations_jdbc ADD CONSTRAINT task_destinations_jdbc_task_destinations_fk FOREIGN KEY (task_destination_id) REFERENCES public.task_destinations(id) ON DELETE CASCADE ON UPDATE CASCADE;