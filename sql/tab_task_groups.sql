DROP TABLE if exists public.tab_task_group;

CREATE TABLE public.tab_task_group (
	task_id varchar NOT NULL,
	group_name varchar NOT NULL,
	CONSTRAINT tab_task_group_pkey PRIMARY KEY (task_id, group_name)
);

ALTER TABLE public.tab_task_group ADD CONSTRAINT tab_task_group_tasks_fk FOREIGN KEY (task_id) REFERENCES public.tab_tasks(id) ON DELETE CASCADE ON UPDATE CASCADE;