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

------------------------------------------------------------------------------------------

drop table if exists public.tab_semaforo_mensile;

CREATE TABLE public.tab_semaforo_mensile (
	id int4 NOT NULL,
	cod_abi int4 NULL,
	periodo_rif int4 NULL,
	tabella varchar(128) NOT NULL,
	tipo_caricamento varchar(8) NOT NULL,
	provenienza bpchar(2) NOT NULL,
	id_file varchar(32) NULL,
	colonna_valore varchar(16) NULL,
	ambito int4 NULL,
	o_carico timestamp(3) NULL,
	CONSTRAINT pk_tab_semaforo_mensile PRIMARY KEY (id)
);

GRANT TRUNCATE, INSERT, SELECT ON public.tab_semaforo_mensile TO nplg_app;
INSERT INTO public.tab_semaforo_mensile (id,cod_abi,periodo_rif,tabella,tipo_caricamento,provenienza,id_file,colonna_valore,ambito,o_carico) VALUES
	 (2,1025,202601,'READDR','Rapporti','0T','','',0,'2026-01-24 20:07:18.877'),
	 (3,1025,202601,'READDR','Rapporti','1L','','',0,'2026-01-24 20:07:18.877'),
	 (4,1025,202601,'READDR','Rapporti','1S','','',0,'2026-01-24 20:07:18.877'),
	 (5,1025,202601,'READDR','Rapporti','2F','','',0,'2026-01-24 20:07:18.877'),
	 (6,1025,202601,'READDR','Rapporti','99','','',0,'2026-01-24 20:07:18.877'),
	 (7,1025,202601,'READDR','Rapporti','AC','','',0,'2026-01-24 20:07:18.877'),
	 (8,1025,202601,'READDR','Rapporti','CT','','',0,'2026-01-24 20:07:18.877'),
	 (9,1025,202601,'READDR','Rapporti','CV','','',0,'2026-01-24 20:07:18.877'),
	 (10,1025,202601,'READDR','Rapporti','EB','','',0,'2026-01-24 20:07:18.877'),
	 (11,1025,202601,'READDR','Rapporti','EC','','',0,'2026-01-24 20:07:18.877');
INSERT INTO public.tab_semaforo_mensile (id,cod_abi,periodo_rif,tabella,tipo_caricamento,provenienza,id_file,colonna_valore,ambito,o_carico) VALUES
	 (12,1025,202601,'READDR','Rapporti','ET','','',0,'2026-01-24 20:07:18.877'),
	 (13,1025,202601,'READDR','Rapporti','G1','','',0,'2026-01-24 20:07:18.877'),
	 (14,1025,202601,'READDR','Rapporti','GH','','',0,'2026-01-24 20:07:18.877'),
	 (15,1025,202601,'READDR','Rapporti','H2','','',0,'2026-01-24 20:07:18.877'),
	 (16,1025,202601,'READDR','Rapporti','H4','','',0,'2026-01-24 20:07:18.877'),
	 (17,1025,202601,'READDR','Rapporti','H8','','',0,'2026-01-24 20:07:18.877'),
	 (18,1025,202601,'READDR','Rapporti','HC','','',0,'2026-01-24 20:07:18.877'),
	 (19,1025,202601,'READDR','Rapporti','HF','','',0,'2026-01-24 20:07:18.877'),
	 (20,1025,202601,'READDR','Rapporti','I4','','',0,'2026-01-24 20:07:18.877'),
	 (21,1025,202601,'READDR','Rapporti','I5','','',0,'2026-01-24 20:07:18.877');
INSERT INTO public.tab_semaforo_mensile (id,cod_abi,periodo_rif,tabella,tipo_caricamento,provenienza,id_file,colonna_valore,ambito,o_carico) VALUES
	 (22,1025,202601,'READDR','Rapporti','JC','','',0,'2026-01-24 20:07:18.877'),
	 (23,1025,202601,'READDR','Rapporti','K1','','',0,'2026-01-24 20:07:18.877'),
	 (24,1025,202601,'READDR','Rapporti','KS','','',0,'2026-01-24 20:07:18.877'),
	 (25,1025,202601,'READDR','Rapporti','L3','','',0,'2026-01-24 20:07:18.877'),
	 (26,1025,202601,'READDR','Rapporti','LQ','','',0,'2026-01-24 20:07:18.877'),
	 (27,1025,202601,'READDR','Rapporti','LX','','',0,'2026-01-24 20:07:18.877'),
	 (28,1025,202601,'READDR','Rapporti','LY','','',0,'2026-01-24 20:07:18.877'),
	 (29,1025,202601,'READDR','Rapporti','M3','','',0,'2026-01-24 20:07:18.877'),
	 (30,1025,202601,'READDR','Rapporti','MO','','',0,'2026-01-24 20:07:18.877'),
	 (31,1025,202601,'READDR','Rapporti','N1','','',0,'2026-01-24 20:07:18.877');
INSERT INTO public.tab_semaforo_mensile (id,cod_abi,periodo_rif,tabella,tipo_caricamento,provenienza,id_file,colonna_valore,ambito,o_carico) VALUES
	 (32,1025,202601,'READDR','Rapporti','NQ','','',0,'2026-01-24 20:07:18.877'),
	 (33,1025,202601,'READDR','Rapporti','PA','','',0,'2026-01-24 20:07:18.877'),
	 (34,1025,202601,'READDR','Rapporti','PI','','',0,'2026-01-24 20:07:18.877'),
	 (35,1025,202601,'READDR','Rapporti','PR','','',0,'2026-01-24 20:07:18.877'),
	 (36,1025,202601,'READDR','Rapporti','SY','','',0,'2026-01-24 20:07:18.877'),
	 (37,1025,202601,'READDR','Rapporti','T2','','',0,'2026-01-24 20:07:18.877'),
	 (38,1025,202601,'READDR','Rapporti','T5','','',0,'2026-01-24 20:07:18.877'),
	 (39,1025,202601,'READDR','Rapporti','TD','','',0,'2026-01-24 20:07:18.877'),
	 (40,1025,202601,'READDR','Rapporti','TR','','',0,'2026-01-24 20:07:18.877'),
	 (41,1025,202601,'READDR','Rapporti','TS','','',0,'2026-01-24 20:07:18.877');
INSERT INTO public.tab_semaforo_mensile (id,cod_abi,periodo_rif,tabella,tipo_caricamento,provenienza,id_file,colonna_valore,ambito,o_carico) VALUES
	 (42,1025,202601,'READDR','Rapporti','TX','','',0,'2026-01-24 20:07:18.877'),
	 (43,1025,202601,'READDR','Rapporti','TY','','',0,'2026-01-24 20:07:18.877'),
	 (44,1025,202601,'READDR','Rapporti','U4','','',0,'2026-01-24 20:07:18.877'),
	 (45,1025,202601,'READDR','Rapporti','V4','','',0,'2026-01-24 20:07:18.877'),
	 (46,1025,202601,'READDR','Rapporti','V5','','',0,'2026-01-24 20:07:18.877'),
	 (47,1025,202601,'READDR','Rapporti','XX','','',0,'2026-01-24 20:07:18.877'),
	 (48,1025,202601,'READDR','Rapporti','06','','',0,'2026-01-24 20:07:18.877'),
	 (49,1025,202601,'READDR','Rapporti','4F','','',0,'2026-01-24 20:07:18.877'),
	 (50,1025,202601,'READDR','Rapporti','B3','','',0,'2026-01-24 20:07:18.877'),
	 (51,1025,202601,'READDR','Rapporti','CA','','',0,'2026-01-24 20:07:18.877');
INSERT INTO public.tab_semaforo_mensile (id,cod_abi,periodo_rif,tabella,tipo_caricamento,provenienza,id_file,colonna_valore,ambito,o_carico) VALUES
	 (52,1025,202601,'READDR','Rapporti','CW','','',0,'2026-01-24 20:07:18.877'),
	 (53,1025,202601,'READDR','Rapporti','FH','','',0,'2026-01-24 20:07:18.877'),
	 (54,1025,202601,'READDR','Rapporti','G2','','',0,'2026-01-24 20:07:18.877'),
	 (55,1025,202601,'READDR','Rapporti','GP','','',0,'2026-01-24 20:07:18.877'),
	 (56,1025,202601,'READDR','Rapporti','H6','','',0,'2026-01-24 20:07:18.877'),
	 (57,1025,202601,'READDR','Rapporti','H7','','',0,'2026-01-24 20:07:18.877'),
	 (58,1025,202601,'READDR','Rapporti','H9','','',0,'2026-01-24 20:07:18.877'),
	 (59,1025,202601,'READDR','Rapporti','HA','','',0,'2026-01-24 20:07:18.877'),
	 (60,1025,202601,'READDR','Rapporti','HB','','',0,'2026-01-24 20:07:18.877'),
	 (61,1025,202601,'READDR','Rapporti','HG','','',0,'2026-01-24 20:07:18.877');
INSERT INTO public.tab_semaforo_mensile (id,cod_abi,periodo_rif,tabella,tipo_caricamento,provenienza,id_file,colonna_valore,ambito,o_carico) VALUES
	 (62,1025,202601,'READDR','Rapporti','I6','','',0,'2026-01-24 20:07:18.877'),
	 (63,1025,202601,'READDR','Rapporti','I7','','',0,'2026-01-24 20:07:18.877'),
	 (64,1025,202601,'READDR','Rapporti','I9','','',0,'2026-01-24 20:07:18.877'),
	 (65,1025,202601,'READDR','Rapporti','IA','','',0,'2026-01-24 20:07:18.877'),
	 (66,1025,202601,'READDR','Rapporti','L1','','',0,'2026-01-24 20:07:18.877'),
	 (67,1025,202601,'READDR','Rapporti','L2','','',0,'2026-01-24 20:07:18.877'),
	 (68,1025,202601,'READDR','Rapporti','LZ','','',0,'2026-01-24 20:07:18.877'),
	 (69,1025,202601,'READDR','Rapporti','M1','','',0,'2026-01-24 20:07:18.877'),
	 (70,1025,202601,'READDR','Rapporti','M2','','',0,'2026-01-24 20:07:18.877'),
	 (71,1025,202601,'READDR','Rapporti','N3','','',0,'2026-01-24 20:07:18.877');
INSERT INTO public.tab_semaforo_mensile (id,cod_abi,periodo_rif,tabella,tipo_caricamento,provenienza,id_file,colonna_valore,ambito,o_carico) VALUES
	 (72,1025,202601,'READDR','Rapporti','TW','','',0,'2026-01-24 20:07:18.877'),
	 (73,1025,202601,'READDR','Rapporti','U1','','',0,'2026-01-24 20:07:18.877'),
	 (74,1025,202601,'READDR','Rapporti','V1','','',0,'2026-01-24 20:07:18.877'),
	 (75,1025,202601,'READDR','Rapporti','V2','','',0,'2026-01-24 20:07:18.877'),
	 (76,1025,202601,'READDR','Rapporti','VX','','',0,'2026-01-24 20:07:18.877'),
	 (77,1025,202601,'READDR','Rapporti','14','','',0,'2026-01-24 20:07:18.877'),
	 (78,1025,202601,'READDR','Rapporti','1F','','',0,'2026-01-24 20:07:18.877'),
	 (79,1025,202601,'READDR','Rapporti','B1','','',0,'2026-01-24 20:07:18.877'),
	 (80,1025,202601,'READDR','Rapporti','BZ','','',0,'2026-01-24 20:07:18.877'),
	 (81,1025,202601,'READDR','Rapporti','CD','','',0,'2026-01-24 20:07:18.877');
INSERT INTO public.tab_semaforo_mensile (id,cod_abi,periodo_rif,tabella,tipo_caricamento,provenienza,id_file,colonna_valore,ambito,o_carico) VALUES
	 (82,1025,202601,'READDR','Rapporti','CI','','',0,'2026-01-24 20:07:18.877'),
	 (83,1025,202601,'READDR','Rapporti','CQ','','',0,'2026-01-24 20:07:18.877'),
	 (84,1025,202601,'READDR','Rapporti','ED','','',0,'2026-01-24 20:07:18.877'),
	 (85,1025,202601,'READDR','Rapporti','EY','','',0,'2026-01-24 20:07:18.877'),
	 (86,1025,202601,'READDR','Rapporti','F2','','',0,'2026-01-24 20:07:18.877'),
	 (87,1025,202601,'READDR','Rapporti','GA','','',0,'2026-01-24 20:07:18.877'),
	 (88,1025,202601,'READDR','Rapporti','H3','','',0,'2026-01-24 20:07:18.877'),
	 (89,1025,202601,'READDR','Rapporti','HD','','',0,'2026-01-24 20:07:18.877'),
	 (90,1025,202601,'READDR','Rapporti','HL','','',0,'2026-01-24 20:07:18.877'),
	 (91,1025,202601,'READDR','Rapporti','HR','','',0,'2026-01-24 20:07:18.877');
INSERT INTO public.tab_semaforo_mensile (id,cod_abi,periodo_rif,tabella,tipo_caricamento,provenienza,id_file,colonna_valore,ambito,o_carico) VALUES
	 (92,1025,202601,'READDR','Rapporti','HS','','',0,'2026-01-24 20:07:18.877'),
	 (93,1025,202601,'READDR','Rapporti','IX','','',0,'2026-01-24 20:07:18.877'),
	 (94,1025,202601,'READDR','Rapporti','KA','','',0,'2026-01-24 20:07:18.877'),
	 (95,1025,202601,'READDR','Rapporti','L4','','',0,'2026-01-24 20:07:18.877'),
	 (96,1025,202601,'READDR','Rapporti','NP','','',0,'2026-01-24 20:07:18.877'),
	 (97,1025,202601,'READDR','Rapporti','P5','','',0,'2026-01-24 20:07:18.877'),
	 (98,1025,202601,'READDR','Rapporti','PT','','',0,'2026-01-24 20:07:18.877'),
	 (99,1025,202601,'READDR','Rapporti','PZ','','',0,'2026-01-24 20:07:18.877'),
	 (100,1025,202601,'READDR','Rapporti','TC','','',0,'2026-01-24 20:07:18.877'),
	 (101,1025,202601,'READDR','Rapporti','TF','','',0,'2026-01-24 20:07:18.877');
INSERT INTO public.tab_semaforo_mensile (id,cod_abi,periodo_rif,tabella,tipo_caricamento,provenienza,id_file,colonna_valore,ambito,o_carico) VALUES
	 (102,1025,202601,'READDR','Rapporti','TJ','','',0,'2026-01-24 20:07:18.877'),
	 (103,1025,202601,'READDR','Rapporti','TM','','',0,'2026-01-24 20:07:18.877'),
	 (104,1025,202601,'READDR','Rapporti','TT','','',0,'2026-01-24 20:07:18.877'),
	 (105,1025,202601,'READDR','Rapporti','TV','','',0,'2026-01-24 20:07:18.877'),
	 (106,1025,202601,'READDR','Rapporti','U3','','',0,'2026-01-24 20:07:18.877'),
	 (107,1025,202601,'READDR','Rapporti','UP','','',0,'2026-01-24 20:07:18.877'),
	 (108,1025,202601,'READDR','Rapporti','V3','','',0,'2026-01-24 20:07:18.877'),
	 (109,1025,202601,'READDR','Rapporti','VR','','',0,'2026-01-24 20:07:18.877'),
	 (110,1025,202601,'READDR','Rapporti','00','','',0,'2026-01-24 20:07:18.877'),
	 (111,1025,202601,'READDR','Rapporti','3F','','',0,'2026-01-24 20:07:18.877');
INSERT INTO public.tab_semaforo_mensile (id,cod_abi,periodo_rif,tabella,tipo_caricamento,provenienza,id_file,colonna_valore,ambito,o_carico) VALUES
	 (112,1025,202601,'READDR','Rapporti','B2','','',0,'2026-01-24 20:07:18.877'),
	 (113,1025,202601,'READDR','Rapporti','B4','','',0,'2026-01-24 20:07:18.877'),
	 (114,1025,202601,'READDR','Rapporti','BF','','',0,'2026-01-24 20:07:18.877'),
	 (115,1025,202601,'READDR','Rapporti','BM','','',0,'2026-01-24 20:07:18.877'),
	 (116,1025,202601,'READDR','Rapporti','G3','','',0,'2026-01-24 20:07:18.877'),
	 (117,1025,202601,'READDR','Rapporti','H0','','',0,'2026-01-24 20:07:18.877'),
	 (118,1025,202601,'READDR','Rapporti','H1','','',0,'2026-01-24 20:07:18.877'),
	 (119,1025,202601,'READDR','Rapporti','HE','','',0,'2026-01-24 20:07:18.877'),
	 (120,1025,202601,'READDR','Rapporti','HO','','',0,'2026-01-24 20:07:18.877'),
	 (121,1025,202601,'READDR','Rapporti','I1','','',0,'2026-01-24 20:07:18.877');
INSERT INTO public.tab_semaforo_mensile (id,cod_abi,periodo_rif,tabella,tipo_caricamento,provenienza,id_file,colonna_valore,ambito,o_carico) VALUES
	 (122,1025,202601,'READDR','Rapporti','KL','','',0,'2026-01-24 20:07:18.877'),
	 (123,1025,202601,'READDR','Rapporti','M6','','',0,'2026-01-24 20:07:18.877'),
	 (124,1025,202601,'READDR','Rapporti','N2','','',0,'2026-01-24 20:07:18.877'),
	 (125,1025,202601,'READDR','Rapporti','N4','','',0,'2026-01-24 20:07:18.877'),
	 (126,1025,202601,'READDR','Rapporti','SM','','',0,'2026-01-24 20:07:18.877'),
	 (127,1025,202601,'READDR','Rapporti','SZ','','',0,'2026-01-24 20:07:18.877'),
	 (128,1025,202601,'READDR','Rapporti','TH','','',0,'2026-01-24 20:07:18.877'),
	 (129,1025,202601,'READDR','Rapporti','TO','','',0,'2026-01-24 20:07:18.877'),
	 (130,1025,202601,'READDR','Rapporti','TU','','',0,'2026-01-24 20:07:18.877'),
	 (131,1025,202601,'READDR','Rapporti','TZ','','',0,'2026-01-24 20:07:18.877');
INSERT INTO public.tab_semaforo_mensile (id,cod_abi,periodo_rif,tabella,tipo_caricamento,provenienza,id_file,colonna_valore,ambito,o_carico) VALUES
	 (132,1025,202601,'READDR','Rapporti','U6','','',0,'2026-01-24 20:07:18.877'),
	 (133,1234,202601,'READDR','Rapporti','CA','','',0,'2026-01-24 20:07:18.877'),
	 (134,3239,202601,'READDR','Rapporti','07','','',0,'2026-01-24 20:07:18.877'),
	 (135,3239,202601,'READDR','Rapporti','B3','','',0,'2026-01-24 20:07:18.877'),
	 (136,3239,202601,'READDR','Rapporti','DD','','',0,'2026-01-24 20:07:18.877'),
	 (137,3239,202601,'READDR','Rapporti','DH','','',0,'2026-01-24 20:07:18.877'),
	 (138,3239,202601,'READDR','Rapporti','G2','','',0,'2026-01-24 20:07:18.877'),
	 (139,3239,202601,'READDR','Rapporti','GP','','',0,'2026-01-24 20:07:18.877'),
	 (140,3239,202601,'READDR','Rapporti','I9','','',0,'2026-01-24 20:07:18.877'),
	 (141,3239,202601,'READDR','Rapporti','M2','','',0,'2026-01-24 20:07:18.877');
INSERT INTO public.tab_semaforo_mensile (id,cod_abi,periodo_rif,tabella,tipo_caricamento,provenienza,id_file,colonna_valore,ambito,o_carico) VALUES
	 (142,3239,202601,'READDR','Rapporti','08','','',0,'2026-01-24 20:07:18.877'),
	 (143,3239,202601,'READDR','Rapporti','99','','',0,'2026-01-24 20:07:18.877'),
	 (144,3239,202601,'READDR','Rapporti','CV','','',0,'2026-01-24 20:07:18.877'),
	 (145,3239,202601,'READDR','Rapporti','DG','','',0,'2026-01-24 20:07:18.877'),
	 (146,3239,202601,'READDR','Rapporti','ET','','',0,'2026-01-24 20:07:18.877'),
	 (147,3239,202601,'READDR','Rapporti','JC','','',0,'2026-01-24 20:07:18.877'),
	 (148,3239,202601,'READDR','Rapporti','N1','','',0,'2026-01-24 20:07:18.877'),
	 (149,3239,202601,'READDR','Rapporti','T5','','',0,'2026-01-24 20:07:18.877'),
	 (150,3239,202601,'READDR','Rapporti','TX','','',0,'2026-01-24 20:07:18.877'),
	 (151,3239,202601,'READDR','Rapporti','U4','','',0,'2026-01-24 20:07:18.877');
INSERT INTO public.tab_semaforo_mensile (id,cod_abi,periodo_rif,tabella,tipo_caricamento,provenienza,id_file,colonna_valore,ambito,o_carico) VALUES
	 (152,3239,202601,'READDR','Rapporti','00','','',0,'2026-01-24 20:07:18.877'),
	 (153,3239,202601,'READDR','Rapporti','B2','','',0,'2026-01-24 20:07:18.877'),
	 (154,3239,202601,'READDR','Rapporti','DB','','',0,'2026-01-24 20:07:18.877'),
	 (155,3239,202601,'READDR','Rapporti','DC','','',0,'2026-01-24 20:07:18.877'),
	 (156,3239,202601,'READDR','Rapporti','DF','','',0,'2026-01-24 20:07:18.877'),
	 (157,3239,202601,'READDR','Rapporti','N2','','',0,'2026-01-24 20:07:18.877'),
	 (158,3239,202601,'READDR','Rapporti','U6','','',0,'2026-01-24 20:07:18.877'),
	 (159,3239,202601,'READDR','Rapporti','23','','',0,'2026-01-24 20:07:18.877'),
	 (160,3239,202601,'READDR','Rapporti','B1','','',0,'2026-01-24 20:07:18.877'),
	 (161,3239,202601,'READDR','Rapporti','CA','','',0,'2026-01-24 20:07:18.877');
INSERT INTO public.tab_semaforo_mensile (id,cod_abi,periodo_rif,tabella,tipo_caricamento,provenienza,id_file,colonna_valore,ambito,o_carico) VALUES
	 (162,3239,202601,'READDR','Rapporti','DE','','',0,'2026-01-24 20:07:18.877'),
	 (163,3239,202601,'READDR','Rapporti','TC','','',0,'2026-01-24 20:07:18.877'),
	 (164,3239,202601,'READDR','Rapporti','TM','','',0,'2026-01-24 20:07:18.877'),
	 (165,3239,202601,'READDR','Rapporti','TT','','',0,'2026-01-24 20:07:18.877'),
	 (166,3239,202601,'READDR','Rapporti','UP','','',0,'2026-01-24 20:07:18.877'),
	 (167,3296,202601,'READDR','Rapporti','1M','','',0,'2026-01-24 20:07:18.877'),
	 (168,3296,202601,'READDR','Rapporti','B3','','',0,'2026-01-24 20:07:18.877'),
	 (169,3296,202601,'READDR','Rapporti','DE','','',0,'2026-01-24 20:07:18.877'),
	 (170,3296,202601,'READDR','Rapporti','DH','','',0,'2026-01-24 20:07:18.877'),
	 (171,3296,202601,'READDR','Rapporti','DO','','',0,'2026-01-24 20:07:18.877');
INSERT INTO public.tab_semaforo_mensile (id,cod_abi,periodo_rif,tabella,tipo_caricamento,provenienza,id_file,colonna_valore,ambito,o_carico) VALUES
	 (172,3296,202601,'READDR','Rapporti','IA','','',0,'2026-01-24 20:07:18.877'),
	 (173,3296,202601,'READDR','Rapporti','M2','','',0,'2026-01-24 20:07:18.877'),
	 (174,3296,202601,'READDR','Rapporti','DD','','',0,'2026-01-24 20:07:18.877'),
	 (175,3296,202601,'READDR','Rapporti','I5','','',0,'2026-01-24 20:07:18.877'),
	 (176,3296,202601,'READDR','Rapporti','I9','','',0,'2026-01-24 20:07:18.877'),
	 (177,3296,202601,'READDR','Rapporti','TC','','',0,'2026-01-24 20:07:18.877'),
	 (178,3296,202601,'READDR','Rapporti','TP','','',0,'2026-01-24 20:07:18.877'),
	 (179,3296,202601,'READDR','Rapporti','U1','','',0,'2026-01-24 20:07:18.877'),
	 (180,3296,202601,'READDR','Rapporti','DA','','',0,'2026-01-24 20:07:18.877'),
	 (181,3296,202601,'READDR','Rapporti','DB','','',0,'2026-01-24 20:07:18.877');
INSERT INTO public.tab_semaforo_mensile (id,cod_abi,periodo_rif,tabella,tipo_caricamento,provenienza,id_file,colonna_valore,ambito,o_carico) VALUES
	 (182,3296,202601,'READDR','Rapporti','DG','','',0,'2026-01-24 20:07:18.877'),
	 (183,3296,202601,'READDR','Rapporti','ET','','',0,'2026-01-24 20:07:18.877'),
	 (184,3296,202601,'READDR','Rapporti','JC','','',0,'2026-01-24 20:07:18.877'),
	 (185,3296,202601,'READDR','Rapporti','T4','','',0,'2026-01-24 20:07:18.877'),
	 (186,3296,202601,'READDR','Rapporti','U4','','',0,'2026-01-24 20:07:18.877'),
	 (187,3296,202601,'READDR','Rapporti','00','','',0,'2026-01-24 20:07:18.877'),
	 (188,3296,202601,'READDR','Rapporti','99','','',0,'2026-01-24 20:07:18.877'),
	 (189,3296,202601,'READDR','Rapporti','B1','','',0,'2026-01-24 20:07:18.877'),
	 (190,3296,202601,'READDR','Rapporti','B2','','',0,'2026-01-24 20:07:18.877'),
	 (191,3296,202601,'READDR','Rapporti','DC','','',0,'2026-01-24 20:07:18.877');
INSERT INTO public.tab_semaforo_mensile (id,cod_abi,periodo_rif,tabella,tipo_caricamento,provenienza,id_file,colonna_valore,ambito,o_carico) VALUES
	 (192,3296,202601,'READDR','Rapporti','DF','','',0,'2026-01-24 20:07:18.877'),
	 (193,3296,202601,'READDR','Rapporti','DN','','',0,'2026-01-24 20:07:18.877'),
	 (194,3296,202601,'READDR','Rapporti','DP','','',0,'2026-01-24 20:07:18.877'),
	 (195,3296,202601,'READDR','Rapporti','N1','','',0,'2026-01-24 20:07:18.877'),
	 (196,3296,202601,'READDR','Rapporti','N2','','',0,'2026-01-24 20:07:18.877'),
	 (197,3296,202601,'READDR','Rapporti','T5','','',0,'2026-01-24 20:07:18.877'),
	 (198,3385,202601,'READDR','Rapporti','ET','','',0,'2026-01-24 20:07:18.877'),
	 (199,3385,202601,'READDR','Rapporti','I5','','',0,'2026-01-24 20:07:18.877'),
	 (200,3385,202601,'READDR','Rapporti','M3','','',0,'2026-01-24 20:07:18.877'),
	 (201,3385,202601,'READDR','Rapporti','MM','','',0,'2026-01-24 20:07:18.877');

--------------------------------------------------------------------------------------
drop table if exists public.tab_semaforo_domini;

CREATE TABLE public.tab_semaforo_domini (
	id int4 NOT NULL,
	tabella varchar(128) NOT NULL,
	o_carico timestamp(3) NULL,
	CONSTRAINT pk_tab_semaforo_domini PRIMARY KEY (id)
);


GRANT TRUNCATE, INSERT, SELECT ON public.tab_semaforo_domini TO nplg_app;
----------------------------------------------------------------------------------------

drop table if exists public.tab_no_semaforo;

CREATE TABLE public.tab_no_semaforo (
	tabella varchar(128) NOT NULL,
    tipo_caricamento varchar(8) NOT NULL DEFAULT 'NO_SEM',
	CONSTRAINT pk_tab_no_semaforo PRIMARY KEY (tabella)
);


GRANT TRUNCATE, INSERT, select ON public.tab_no_semaforo TO nplg_app;

INSERT INTO public.tab_no_semaforo (tabella,tipo_caricamento) VALUES
	 ('NPLG0_SEMAFORO_MENSILE','Semaforo'),
	 ('NPLG0_SEMAFORO_DOMINI','Semaforo');

-----------------------------------------------------------------------------------------
DROP TABLE if exists public.tab_semaforo_steps;

CREATE TABLE public.tab_semaforo_steps (
	uid uuid NULL,
	run_id varchar NOT NULL,
	logical_table varchar(128) NOT NULL,
	tipo_caricamento varchar NOT NULL,
	"key" jsonb NOT NULL,
	query_param jsonb NOT NULL,
	layer varchar NULL
);

GRANT TRUNCATE, INSERT, select ON public.tab_semaforo_steps TO nplg_app;
------------------------------------------------------------------------------------------
DROP TABLE if exists public.tab_task_configs;

CREATE TABLE public.tab_task_configs (
	"key" jsonb NOT NULL,
	description varchar NULL,
	main_python_file varchar NOT NULL,
	additional_python_file_uris _varchar NULL,
	jar_file_uris _varchar NULL,
	additional_file_uris _varchar NULL,
	archive_file_uris _varchar NULL,
	logging_config json NULL,
	dataproc_properties json NULL,
	is_heavy bool DEFAULT false NULL,
	layer varchar DEFAULT 'stage'::character varying NOT NULL,
	CONSTRAINT tab_task_configs_pkey PRIMARY KEY (key, layer)
);
GRANT SELECT ON table public.tab_task_configs TO nplg_app;

INSERT INTO public.tab_task_configs ("key",description,main_python_file,additional_python_file_uris,jar_file_uris,additional_file_uris,archive_file_uris,logging_config,dataproc_properties,is_heavy,layer) VALUES
	 ('{"cod_abi": 1025, "cod_tabella": "READDR", "cod_provenienza": "N1"}','configurazione stage per READDR banca 1025 provenienza N1','gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/main_processor.py','{gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/ingestion_process-1.0.0-py3-none-any.whl}','{gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/jar/mssql-jdbc-12.2.0.jre11.jar,gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/jar/postgresql-42.7.8.jar}',NULL,NULL,NULL,'{ "spark.driver.cores" : "2","spark.driver.memory": "4g", "spark.executor.memory": "12g", "spark.executor.cores": "4",
 "spark.default.parallelism": "12","spark.ui.enabled" : "true", "spark.sql.adaptive.enabled": "true", "spark.sql.adaptive.coalescePartitions.enabled":"true",
"spark.sql.adaptive.skewJoin.enabled":"true","spark.sql.shuffle.partitions": "12","spark.dynamicAllocation.enabled":"true","spark.shuffle.service.enabled":"true",
"spark.dynamicAllocation.minExecutors":"1","spark.dynamicAllocation.initialExecutors":"3","spark.dynamicAllocation.maxExecutors":"5","spark.submit.deployMode":"cluster"}',true,'stage'),
	 ('{"cod_abi": 1025, "cod_tabella": "READDR", "cod_provenienza": "N3"}','configurazione stage per READDR banca 1025 provenienza N3','gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/main_processor.py','{gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/ingestion_process-1.0.0-py3-none-any.whl}','{gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/jar/mssql-jdbc-12.2.0.jre11.jar,gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/jar/postgresql-42.7.8.jar}',NULL,NULL,NULL,'{ "spark.driver.cores" : "2","spark.driver.memory": "4g", "spark.executor.memory": "12g", "spark.executor.cores": "4",
 "spark.default.parallelism": "12","spark.ui.enabled" : "true", "spark.sql.adaptive.enabled": "true", "spark.sql.adaptive.coalescePartitions.enabled":"true",
"spark.sql.adaptive.skewJoin.enabled":"true","spark.sql.shuffle.partitions": "12","spark.dynamicAllocation.enabled":"true","spark.shuffle.service.enabled":"true",
"spark.dynamicAllocation.minExecutors":"1","spark.dynamicAllocation.initialExecutors":"3","spark.dynamicAllocation.maxExecutors":"5","spark.submit.deployMode":"cluster"}',true,'stage'),
	 ('{"cod_abi": 1025, "cod_tabella": "READDR", "cod_provenienza": "TF"}','configurazione stage per READDR banca 1025 provenienza TF','gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/main_processor.py','{gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/ingestion_process-1.0.0-py3-none-any.whl}','{gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/jar/mssql-jdbc-12.2.0.jre11.jar,gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/jar/postgresql-42.7.8.jar}',NULL,NULL,NULL,'{ "spark.driver.cores" : "2","spark.driver.memory": "4g", "spark.executor.memory": "12g", "spark.executor.cores": "4",
 "spark.default.parallelism": "12","spark.ui.enabled" : "true", "spark.sql.adaptive.enabled": "true", "spark.sql.adaptive.coalescePartitions.enabled":"true",
"spark.sql.adaptive.skewJoin.enabled":"true","spark.sql.shuffle.partitions": "12","spark.dynamicAllocation.enabled":"true","spark.shuffle.service.enabled":"true",
"spark.dynamicAllocation.minExecutors":"1","spark.dynamicAllocation.initialExecutors":"3","spark.dynamicAllocation.maxExecutors":"5","spark.submit.deployMode":"cluster"}',true,'stage'),
	 ('{"cod_abi": 1025, "cod_tabella": "READDR", "cod_provenienza": "VR"}','configurazione stage per READDR banca 1025 provenienza VR','gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/main_processor.py','{gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/ingestion_process-1.0.0-py3-none-any.whl}','{gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/jar/mssql-jdbc-12.2.0.jre11.jar,gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/jar/postgresql-42.7.8.jar}',NULL,NULL,NULL,'{ "spark.driver.cores" : "2","spark.driver.memory": "4g", "spark.executor.memory": "12g", "spark.executor.cores": "4",
 "spark.default.parallelism": "12","spark.ui.enabled" : "true", "spark.sql.adaptive.enabled": "true", "spark.sql.adaptive.coalescePartitions.enabled":"true",
"spark.sql.adaptive.skewJoin.enabled":"true","spark.sql.shuffle.partitions": "12","spark.dynamicAllocation.enabled":"true","spark.shuffle.service.enabled":"true",
"spark.dynamicAllocation.minExecutors":"1","spark.dynamicAllocation.initialExecutors":"3","spark.dynamicAllocation.maxExecutors":"5","spark.submit.deployMode":"cluster"}',true,'stage'),
	 ('{"cod_abi": 3296, "cod_tabella": "READDR", "cod_provenienza": "DN"}','configurazione stage per READDR banca 3296 provenienza DP','gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/main_processor.py','{gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/ingestion_process-1.0.0-py3-none-any.whl}','{gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/jar/mssql-jdbc-12.2.0.jre11.jar,gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/jar/postgresql-42.7.8.jar}',NULL,NULL,NULL,'{ "spark.driver.cores" : "2","spark.driver.memory": "4g", "spark.executor.memory": "12g", "spark.executor.cores": "4",
 "spark.default.parallelism": "12","spark.ui.enabled" : "true", "spark.sql.adaptive.enabled": "true", "spark.sql.adaptive.coalescePartitions.enabled":"true",
"spark.sql.adaptive.skewJoin.enabled":"true","spark.sql.shuffle.partitions": "12","spark.dynamicAllocation.enabled":"true","spark.shuffle.service.enabled":"true",
"spark.dynamicAllocation.minExecutors":"1","spark.dynamicAllocation.initialExecutors":"3","spark.dynamicAllocation.maxExecutors":"5","spark.submit.deployMode":"cluster"}',true,'stage'),
	 ('{"cod_abi": 3296, "cod_tabella": "READDR", "cod_provenienza": "DP"}','configurazione stage per READDR banca 3296 provenienza DP','gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/main_processor.py','{gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/ingestion_process-1.0.0-py3-none-any.whl}','{gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/jar/mssql-jdbc-12.2.0.jre11.jar,gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/jar/postgresql-42.7.8.jar}',NULL,NULL,NULL,'{ "spark.driver.cores" : "2","spark.driver.memory": "4g", "spark.executor.memory": "12g", "spark.executor.cores": "4",
 "spark.default.parallelism": "12","spark.ui.enabled" : "true", "spark.sql.adaptive.enabled": "true", "spark.sql.adaptive.coalescePartitions.enabled":"true",
"spark.sql.adaptive.skewJoin.enabled":"true","spark.sql.shuffle.partitions": "12","spark.dynamicAllocation.enabled":"true","spark.shuffle.service.enabled":"true",
"spark.dynamicAllocation.minExecutors":"1","spark.dynamicAllocation.initialExecutors":"3","spark.dynamicAllocation.maxExecutors":"5","spark.submit.deployMode":"cluster"}',true,'stage'),
	 ('{"cod_abi": 1025, "cod_tabella": "READDR", "cod_provenienza": "CA"}','configurazione stage per READDR banca 1025 provenienza CA','gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/main_processor.py','{gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/ingestion_process-1.0.0-py3-none-any.whl}','{gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/jar/mssql-jdbc-12.2.0.jre11.jar,gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/jar/postgresql-42.7.8.jar}',NULL,NULL,NULL,'{ "spark.driver.cores" : "2","spark.driver.memory": "4g", "spark.executor.memory": "12g", "spark.executor.cores": "4",
 "spark.default.parallelism": "12","spark.ui.enabled" : "true", "spark.sql.adaptive.enabled": "true", "spark.sql.adaptive.coalescePartitions.enabled":"true",
"spark.sql.adaptive.skewJoin.enabled":"true","spark.sql.shuffle.partitions": "12","spark.dynamicAllocation.enabled":"true","spark.shuffle.service.enabled":"true",
"spark.dynamicAllocation.minExecutors":"1","spark.dynamicAllocation.initialExecutors":"3","spark.dynamicAllocation.maxExecutors":"5","spark.submit.deployMode":"cluster"}',true,'stage'),
	 ('{"cod_abi": 1025, "cod_tabella": "READDR", "cod_provenienza": "M2"}','configurazione stage per READDR banca 1025 provenienza M2','gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/main_processor.py','{gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/ingestion_process-1.0.0-py3-none-any.whl}','{gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/jar/mssql-jdbc-12.2.0.jre11.jar,gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/jar/postgresql-42.7.8.jar}',NULL,NULL,NULL,'{ "spark.driver.cores" : "2","spark.driver.memory": "4g", "spark.executor.memory": "12g", "spark.executor.cores": "4",
 "spark.default.parallelism": "12","spark.ui.enabled" : "true", "spark.sql.adaptive.enabled": "true", "spark.sql.adaptive.coalescePartitions.enabled":"true",
"spark.sql.adaptive.skewJoin.enabled":"true","spark.sql.shuffle.partitions": "12","spark.dynamicAllocation.enabled":"true","spark.shuffle.service.enabled":"true",
"spark.dynamicAllocation.minExecutors":"1","spark.dynamicAllocation.initialExecutors":"3","spark.dynamicAllocation.maxExecutors":"5","spark.submit.deployMode":"cluster"}',true,'stage'),
	 ('{"cod_abi": 1025, "cod_tabella": "REAGDG", "cod_provenienza": "AN"}','configurazione stage per REAGDG banca 1025','gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/main_processor.py','{gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/ingestion_process-1.0.0-py3-none-any.whl}','{gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/jar/mssql-jdbc-12.2.0.jre11.jar,gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/jar/postgresql-42.7.8.jar}',NULL,NULL,NULL,'{ "spark.driver.cores" : "2","spark.driver.memory": "4g", "spark.executor.memory": "12g", "spark.executor.cores": "4",
 "spark.default.parallelism": "12","spark.ui.enabled" : "true", "spark.sql.adaptive.enabled": "true", "spark.sql.adaptive.coalescePartitions.enabled":"true",
"spark.sql.adaptive.skewJoin.enabled":"true","spark.sql.shuffle.partitions": "12","spark.dynamicAllocation.enabled":"true","spark.shuffle.service.enabled":"true",
"spark.dynamicAllocation.minExecutors":"1","spark.dynamicAllocation.initialExecutors":"3","spark.dynamicAllocation.maxExecutors":"5",
"spark.sql.adaptive.coalescePartitions.enabled":"false","spark.submit.deployMode":"cluster"}',true,'stage'),
	 ('{}','configurazione silver di default','gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/main_processor.py','{gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/ingestion_process-1.0.0-py3-none-any.whl}','{}',NULL,NULL,NULL,'{}',false,'silver');
INSERT INTO public.tab_task_configs ("key",description,main_python_file,additional_python_file_uris,jar_file_uris,additional_file_uris,archive_file_uris,logging_config,dataproc_properties,is_heavy,layer) VALUES
	 ('{}','configurazione stage di default','gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/main_processor.py','{gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/ingestion_process-1.0.0-py3-none-any.whl}','{gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/jar/mssql-jdbc-12.2.0.jre11.jar,gs://bkt-isp-nplg0-appl-svil-001-ew12/test_workflow/jar/postgresql-42.7.8.jar}',NULL,NULL,NULL,'{ "spark.driver.cores" : "1","spark.driver.memory": "2g", "spark.executor.memory": "12g", "spark.executor.cores": "4",
 "spark.default.parallelism": "12","spark.ui.enabled" : "true", "spark.sql.adaptive.enabled": "true", "spark.sql.adaptive.coalescePartitions.enabled":"true",
"spark.sql.adaptive.skewJoin.enabled":"true","spark.sql.shuffle.partitions": "12","spark.dynamicAllocation.enabled":"true","spark.shuffle.service.enabled":"true",
"spark.dynamicAllocation.minExecutors":"1","spark.dynamicAllocation.initialExecutors":"3","spark.dynamicAllocation.maxExecutors":"5","spark.submit.deployMode":"cluster"}',false,'stage');
-------------------------------------------------------------------------------------------------------------------------------------------
DROP TABLE if exists public.tab_registro_mensile;

CREATE TABLE public.tab_registro_mensile (
    chiave JSONB PRIMARY KEY,
    last_id BIGINT NOT NULL,
    max_data_va INT4,
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

GRANT SELECT,insert,update,delete,truncate ON table public.tab_semaforo_ready TO nplg_app;
-----------------------------------------------------------------------------------------
CREATE TABLE public.tab_table_configs (
	logical_table varchar NOT NULL,
	real_table varchar NULL,
	processor_type varchar DEFAULT 'spark'::character varying NOT NULL,
	layer varchar NOT NULL,
	has_next bool NULL,
	is_blocking bool NULL,
	CONSTRAINT tab_table_configs_layer_check CHECK (((layer)::text = ANY (ARRAY[('stage'::character varying)::text, ('silver'::character varying)::text, ('STAGE'::character varying)::text, ('SILVER'::character varying)::text]))),
	CONSTRAINT tab_table_configs_pk PRIMARY KEY (logical_table, layer),
	CONSTRAINT tab_table_configs_processor_type_check CHECK (((processor_type)::text = ANY (ARRAY[('spark'::character varying)::text, ('bigquery'::character varying)::text, ('SPARK'::character varying)::text, ('BIGQUERY'::character varying)::text, ('NATIVE'::character varying)::text, ('native'::character varying)::text])))
);

GRANT SELECT,ON table public.tab_table_configs TO nplg_app;


INSERT INTO public.tab_table_configs (logical_table,real_table,processor_type,layer,has_next,is_blocking) VALUES
	 ('REAGDG','REAGDG_STG','spark','stage',true,true),
	 ('READDR','READDR_SIL','bigquery','silver',true,true),
	 ('REAGDG','REAGDG_SIL','bigquery','silver',true,true),
	 ('READDR','READDR_STG','spark','stage',true,true),
	 ('NPLG0_SEMAFORO_MENSILE','NPLG0_SEMAFORO_MENSILE','native','stage',false,true),
	 ('NPLG0_SEMAFORO_DOMINI','NPLG0_SEMAFORO_DOMINI','native','stage',false,true);

-----------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW public.vw_semaforo_stage_ready
AS SELECT tb1.uid,
    tb1.logical_table,
    tb1.source_id,
    tb1.destination_id,
    tb1.tipo_caricamento,
    tb1.key,
    tb1.query_param,
    COALESCE(tab_task_configs.is_heavy, false) AS is_heavy
   FROM ( SELECT gen_random_uuid() AS uid,
            s.tabella AS logical_table,
            s.tabella AS source_id,
            s.real_table AS destination_id,
            s.tipo_caricamento,
            s.key,
                CASE
                    WHEN s.tipo_caricamento::text = ANY (ARRAY['TABUT'::character varying::text, 'Tabut'::character varying::text, 'Estraz'::character varying::text, 'ESTRAZ'::character varying::text, 'DOMINI'::character varying::text, 'Domini'::character varying::text]) THEN jsonb_build_object('id', s.id)
                    ELSE jsonb_build_object('id', s.id, 'cod_abi', s.cod_abi, 'cod_provenienza', s.provenienza, 'num_periodo_rif', s.periodo_rif, 'cod_colonna_valore', s.colonna_valore, 'num_ambito', s.ambito, 'max_data_va',
                    CASE
                        WHEN s.tipo_caricamento::text = ANY (ARRAY['CLIENTI'::character varying::text, 'Clienti'::character varying::text]) THEN COALESCE(r.max_data_va, 20000101::bigint)
                        ELSE COALESCE(r.max_data_va, '20000101000000000'::bigint)
                    END)
                END AS query_param
           FROM ( SELECT s_1.id,
                    s_1.tabella,
                    ttc.real_table,
                    s_1.tipo_caricamento,
                    s_1.colonna_valore,
                    s_1.ambito,
                    s_1.cod_abi,
                    s_1.provenienza,
                    s_1.periodo_rif,
                        CASE
                            WHEN s_1.tipo_caricamento::text = ANY (ARRAY['TABUT'::character varying::text, 'Tabut'::character varying::text, 'Estraz'::character varying::text, 'ESTRAZ'::character varying::text]) THEN jsonb_build_object('cod_tabella', s_1.tabella)
                            ELSE jsonb_build_object('cod_abi', s_1.cod_abi, 'cod_tabella', s_1.tabella, 'cod_provenienza', s_1.provenienza)
                        END AS key
                   FROM public.tab_semaforo_mensile s_1
                     LEFT JOIN public.tab_table_configs ttc ON s_1.tabella::text = ttc.logical_table::text
                  WHERE ttc.layer::text = 'stage'::text) s
             LEFT JOIN public.tab_registro_mensile r ON r.chiave = s.key
          WHERE r.last_id IS NULL OR s.id > r.last_id
        UNION ALL
         SELECT gen_random_uuid() AS uid,
            s.tabella AS logical_table,
            s.tabella AS source_id,
            s.real_table AS destination_id,
            s.tipo_caricamento,
            s.key,
            jsonb_build_object('id', s.id) AS query_param
           FROM ( SELECT s_1.id,
                    s_1.tabella,
                    ttc.real_table,
                    s_1.tipo_caricamento,
                    s_1.colonna_valore,
                    s_1.ambito,
                    s_1.cod_abi,
                    s_1.provenienza,
                    s_1.periodo_rif,
                    jsonb_build_object('cod_tabella', s_1.tabella) AS key
                   FROM public.tab_semaforo_domini s_1
                     LEFT JOIN public.tab_table_configs ttc ON s_1.tabella::text = ttc.logical_table::text
                  WHERE ttc.layer::text = 'stage'::text) s
             LEFT JOIN public.tab_registro_mensile r ON r.chiave = s.key
          WHERE r.last_id IS NULL OR s.id > r.last_id
        UNION ALL
         SELECT gen_random_uuid() AS uid,
            s.tabella AS logical_table,
            s.tabella AS source_id,
            ttc.real_table AS destination_id,
            s.tipo_caricamento,
             jsonb_build_object('cod_tabella', s_2.tabella) AS key,
             '{}'::jsonb AS query_param
           FROM public.tab_no_semaforo s_2
             LEFT JOIN public.tab_table_configs ttc ON s.tabella::text = ttc.logical_table::text
          WHERE ttc.layer::text = 'stage'::text) tb1
     LEFT JOIN public.tab_task_configs ON tb1.key = tab_task_configs.key
---------------------------------------------------------------------------------
drop table if exists public.tab_task_sources;

CREATE TABLE public.tab_task_sources (
	id varchar NOT NULL,
    source_id varchar NOT null UNIQUE,
	source_type varchar NOT NULL,
	CONSTRAINT chk_source_type CHECK (((source_type)::text = ANY ((ARRAY['jdbc'::character varying, 'bigquery'::character varying, 'file'::character varying, 'JDBC'::character varying, 'BIGQUERY'::character varying, 'FILE'::character varying])::text[]))),
	CONSTRAINT tab_task_sources_pk PRIMARY KEY (source_id)
);

GRANT SELECT ON table public.tab_task_sources TO nplg_app;

INSERT INTO public.tab_task_sources (id,source_id,source_type) VALUES
	 ('REAGDG','REAGDG','JDBC'),
	 ('READDR','READDR','JDBC'),
	 ('REAGDG_STG','REAGDG_STG','BIGQUERY'),
	 ('READDR_STG','READDR_STG','BIGQUERY'),
	 ('NPLG0_SEMAFORO_MENSILE','NPLG0_SEMAFORO_MENSILE','JDBC'),
	 ('NPLG0_SEMAFORO_DOMINI','NPLG0_SEMAFORO_DOMINI','JDBC');


drop table if exists public.tab_task_destinations;

CREATE TABLE public.tab_task_destinations (
	id varchar NOT NULL,
    destination_id varchar NOT null UNIQUE,
	destination_type varchar NOT NULL,
	CONSTRAINT chk_destination_type CHECK (((destination_type)::text = ANY ((ARRAY['jdbc'::character varying, 'bigquery'::character varying, 'file'::character varying, 'JDBC'::character varying, 'BIGQUERY'::character varying, 'FILE'::character varying])::text[]))),
	CONSTRAINT tab_task_destinations_pk PRIMARY KEY (destination_id)
);


GRANT SELECT ON table public.tab_task_destinations TO nplg_app;

INSERT INTO public.tab_task_destinations (id,destination_id,destination_type) VALUES
	 ('REAGDG_STG','REAGDG_STG','bigquery'),
	 ('READDR_STG','READDR_STG','bigquery'),
	 ('REAGDG_SIL','REAGDG_SIL','bigquery'),
	 ('READDR_SIL','READDR_SIL','bigquery'),
	 ('NPLG0_SEMAFORO_DOMINI','NPLG0_SEMAFORO_DOMINI','jdbc'),
	 ('NPLG0_SEMAFORO_MENSILE','NPLG0_SEMAFORO_MENSILE','jdbc');


drop table if exists public.tab_jdbc_sources;

CREATE TABLE public.tab_jdbc_sources (
	source_id varchar NOT NULL,
	url varchar NOT NULL,
	username varchar NOT NULL,
	pwd varchar NOT NULL,
	driver varchar NOT NULL,
	tablename varchar NULL,
	query_text varchar NULL,
	CONSTRAINT check_one_column_populated CHECK ((((tablename IS NOT NULL) AND (query_text IS NULL)) OR ((tablename IS NULL) AND (query_text IS NOT NULL)))),
	CONSTRAINT tab_jdbc_sources_pk PRIMARY KEY (source_id),
	CONSTRAINT fk_tab_jdbc_sources FOREIGN KEY (source_id) REFERENCES public.tab_task_sources(source_id) ON DELETE CASCADE ON UPDATE CASCADE
);

GRANT SELECT ON table public.tab_jdbc_sources TO nplg_app;

INSERT INTO public.tab_jdbc_sources (source_id,url,username,pwd,driver,tablename,query_text) VALUES
	 ('REAGDG','jdbc:sqlserver://pdbclt076.syssede.systest.sanpaoloimi.com\\\\SYD202:1433;DatabaseName=RDBP0_MENS;encrypt=true;trustServerCertificate=true;integratedSecurity=true;authenticationScheme=NTLM;','SYS_LG_RDB@SYSSPIMI','SECRET::SYS_LG_RDB@SYSSPIMI','com.microsoft.sqlserver.jdbc.SQLServerDriver',NULL,$$'SELECT BANCA as NUM_BANCA,LTRIM(RTRIM(NDG)) as COD_NDG,LTRIM(RTRIM(INTESTAZIONE)) as COD_INTESTAZIONE,LTRIM(RTRIM(CODICE_FISCALE)) as COD_FISCALE,LTRIM(RTRIM(COD_TIP_NDG)) as COD_TIP_NDG,LTRIM(RTRIM(COD_PIVA)) as COD_PIVA,LTRIM(RTRIM(COD_STATO_NDG)) as COD_STATO_NDG,LTRIM(RTRIM(COD_SPECIE_GIURIDICA)) as COD_SPECIE_GIURIDICA, LTRIM(RTRIM(COD_UO_CAPOFILA)) as COD_UO_CAPOFILA, LTRIM(RTRIM(DATA_NASC_COST)) as COD_DATA_NASC_COST,LTRIM(RTRIM(COD_COMUNE_NASCITA)) as COD_COMUNE_NASCITA, LTRIM(RTRIM(COD_PROVNC_NASCT_COST)) as COD_PROVNC_NASCT_COST,LTRIM(RTRIM(COD_CAP_RESIDENZA)) as COD_CAP_RESIDENZA, LTRIM(RTRIM(DES_COMUNE_RESIDENZA)) as DES_COMUNE_RESIDENZA,LTRIM(RTRIM(COD_PROVINCIA_RESIDENZA)) as COD_PROVINCIA_RESIDENZA, LTRIM(RTRIM(COD_SOTTOSEGMENTO_ECONOMICO)) as COD_SOTTOSEGMENTO_ECONOMICO,LTRIM(RTRIM(COD_SOTTOSEGMENTO_COMMERCIALE)) as COD_SOTTOSEGMENTO_COMMERCIALE, LTRIM(RTRIM(COD_CLI_NOPROFIT)) as COD_CLI_NOPROFIT,LTRIM(RTRIM(COD_SEGMENTO_ECONOMICO)) as COD_SEGMENTO_ECONOMICO, LTRIM(RTRIM(COD_PTF_CDG)) as COD_PTF_CDG, LTRIM(RTRIM(COD_UTENZA_MODULO)) as COD_UTENZA_MODULO,LTRIM(RTRIM(COD_SEGM_CDG)) as COD_SEGM_CDG, LTRIM(RTRIM(COD_DESK_CLIENTE)) as COD_DESK_CLIENTE, LTRIM(RTRIM(CLASSE_SOA_UO_PTF)) as COD_CLASSE_SOA_UO_PTF,LTRIM(RTRIM(COD_MODULO_CDG)) as COD_MODULO_CDG, LTRIM(RTRIM(COD_GESTORE_REMOTO)) as COD_GESTORE_REMOTO, LTRIM(RTRIM(RIF_REGOLA_DESK)) as COD_RIF_REGOLA_DESK,LTRIM(RTRIM(COD_BU_TERRTRL)) as COD_BU_TERRTRL, LTRIM(RTRIM(COD_TIP_GESTR_GRM)) as COD_TIP_GESTR_GRM, LTRIM(RTRIM(COD_ATECO)) as COD_ATECO,LTRIM(RTRIM(COD_TIP_GESTR_INDUSTRY)) as COD_TIP_GESTR_INDUSTRY, LTRIM(RTRIM(COD_RAE)) as COD_RAE, LTRIM(RTRIM(COD_BU)) as COD_BU, LTRIM(RTRIM(COD_SAE)) as COD_SAE,LTRIM(RTRIM(COD_GRM)) as COD_GRM, LTRIM(RTRIM(COD_SESSO)) as COD_SESSO, LTRIM(RTRIM(COD_UTENZA_GRM)) as COD_UTENZA_GRM,LTRIM(RTRIM(COD_CAPO_INDUSTRY)) as COD_CAPO_INDUSTRY, LTRIM(RTRIM(COD_NDG_PREVLNT)) as COD_NDG_PREVLNT, LTRIM(RTRIM(COD_ABI_SNDG_NDG_PREV)) as COD_ABI_SNDG_NDG_PREV,LTRIM(RTRIM(COD_SNDG_NDG_PREV)) as COD_SNDG_NDG_PREV, LTRIM(RTRIM(FLG_RESIDENTE)) as COD_FLG_RESIDENTE, LTRIM(RTRIM(COD_RILVNZ_ARTCL_136_TUB)) as COD_RILVNZ_ARTCL_136_TUB,LTRIM(RTRIM(COD_RILVNZ_ARTCL_53_TUB)) as COD_RILVNZ_ARTCL_53_TUB, LTRIM(RTRIM(COD_RILVNZ_CONSOB)) as COD_RILVNZ_CONSOB, LTRIM(RTRIM(COD_RILVNZ_IAS_24)) as COD_RILVNZ_IAS_24,LTRIM(RTRIM(COD_NDG_NO_PUSP)) as COD_NDG_NO_PUSP, LTRIM(RTRIM(COD_TIP_GESTN)) as COD_TIP_GESTN, LTRIM(RTRIM(FLG_CAPO_FILR)) as COD_FLG_CAPO_FILR,LTRIM(RTRIM(FLG_APPRTNZ_FILR)) as COD_FLG_APPRTNZ_FILR, LTRIM(RTRIM(COD_SAG)) as COD_SAG, LTRIM(RTRIM(MACRO_CLUSTER_RATING)) as COD_MACRO_CLUSTER_RATING,LTRIM(RTRIM(CLUSTER_RATING)) as COD_CLUSTER_RATING, LTRIM(RTRIM(CLASSE_UNICA_RATING)) as COD_CLASSE_UNICA_RATING, LTRIM(RTRIM(PD_MEDIA)) as COD_PD_MEDIA,LTRIM(RTRIM(PROV_RATING)) as COD_PROV_RATING, LTRIM(RTRIM(COD_COLORE_CLASSE_CRA)) as COD_COLORE_CLASSE_CRA,LTRIM(RTRIM(COD_CLASSE_RATING_PIU_CRA)) as COD_CLASSE_RATING_PIU_CRA, LTRIM(RTRIM(COD_CDN)) as COD_CDN,LTRIM(RTRIM(SEGMENTO_REGOLAMENTARE)) as COD_SEGMENTO_REGOLAMENTARE, LTRIM(RTRIM(SEGMENTO_COMPORTAMENTALE)) as COD_SEGMENTO_COMPORTAMENTALE,LTRIM(RTRIM(FLG_CONDIVISO)) as COD_FLG_CONDIVISO,case when convert(numeric, data_va) <=20000000 then 20010101 else coalesce(convert(numeric, data_va),20010101) end as NUM_DATA_VA,DATA_INS as NUM_DATA_INS,PERIODO_RIF as NUM_PERIODO_RIF, ${num_periodo_rif} as NUM_PERIODO_COMP FROM REAGDG with (nolock) where BANCA=${cod_abi} and case when convert(int, data_va) <=20000000 then 20010101 else coalesce(convert(int, data_va),20010101) end >=${max_data_va}'$$),
	 ('READDR','jdbc:sqlserver://pdbclt076.syssede.systest.sanpaoloimi.com\\\\SYD202:1433;DatabaseName=RDBP0_MENS;encrypt=true;trustServerCertificate=true;integratedSecurity=true;authenticationScheme=NTLM;','SYS_LG_RDB@SYSSPIMI','SECRET::SYS_LG_RDB@SYSSPIMI','com.microsoft.sqlserver.jdbc.SQLServerDriver',NULL,$$'SELECT BANCA as NUM_BANCA, LTRIM(RTRIM(COD_UO)) as COD_UO,LTRIM(RTRIM(NUM_PARTITA)) as COD_NUM_PARTITA,COD_PROVN_DATI_RED,LTRIM(RTRIM(FORMA_TECNICA)) as COD_FORMA_TECNICA,LTRIM(RTRIM(COD_DIVISA)) as COD_DIVISA,LTRIM(RTRIM(NDG)) as COD_NDG,LTRIM(RTRIM(PTF_RAPPORTO)) as COD_PTF_RAPPORTO, LTRIM(RTRIM(ID_MIGRZ_KTO)) as COD_ID_MIGRZ_KTO,LTRIM(RTRIM(CANALE_VENDITA)) as COD_CANALE_VENDITA,LTRIM(RTRIM(SUB_ID_MIGR_RAPPORTO)) as COD_SUB_ID_MIGR_RAPPORTO,LTRIM(RTRIM(UO_PTF_RAPPORTO)) as COD_UO_PTF_RAPPORTO,LTRIM(RTRIM(COD_STATO_KTO)) as COD_STATO_KTO,LTRIM(RTRIM(COD_PRODOTTO_LEGACY)) as COD_PRODOTTO_LEGACY,LTRIM(RTRIM(COD_PRODOTTO_RAPPORTO)) as COD_PRODOTTO_RAPPORTO,LTRIM(RTRIM(DESK_RAPPORTO)) as COD_DESK_RAPPORTO,LTRIM(RTRIM(COD_RAPP_ANAG)) as COD_RAPP_ANAG,LTRIM(RTRIM(COD_TIP_TASSO)) as COD_TIP_TASSO,COALESCE(TRY_CONVERT(int, NULLIF(LTRIM(RTRIM(DATA_ACCENSIONE)), ''NA'')),0) AS NUM_DATA_ACCENSIONE,COALESCE(TRY_CONVERT(int, NULLIF(LTRIM(RTRIM(DATA_ESTINZIONE)), ''NA'')),0) AS NUM_DATA_ESTINZIONE,COALESCE(TRY_CONVERT(int, NULLIF(LTRIM(RTRIM(DATA_SCADENZA)), ''NA'')),0) AS NUM_DATA_SCADENZA,LTRIM(RTRIM(PTF_SPECCHIO)) as COD_PTF_SPECCHIO,LTRIM(RTRIM(COD_CANALE_PROPOSTA)) as COD_CANALE_PROPOSTA,LTRIM(RTRIM(PRIORITA_DESK_RAPPORTO)) as COD_PRIORITA_DESK_RAPPORTO,LTRIM(RTRIM(DESK_RENDICONTATIVO_RAPPORTO)) as COD_DESK_RENDICONTATIVO_RAPPORTO,LTRIM(RTRIM(PRODOTTO_COMMERCIALE_DINAMICO)) as COD_PRODOTTO_COMMERCIALE_DINAMICO,LTRIM(RTRIM(COD_DIVISIONE)) as COD_DIVISIONE,LTRIM(RTRIM(COD_DIVISIONE_COMMERCIALE)) as COD_DIVISIONE_COMMERCIALE,cast(format(DATAORA_UPD,''yyyyMMddHHmmssfff'') as decimal) as NUM_DATA_VA,DATA_INS as NUM_DATA_INS,case when convert(int, periodo_rif) <=200000 then 200101 else coalesce(convert(int, periodo_rif),200101) end as NUM_PERIODO_RIF, ${num_periodo_rif} as NUM_PERIODO_COMP from READDR with (nolock) where BANCA=${cod_abi} and COD_PROVN_DATI_RED = ''${cod_provenienza}'' and DATAORA_UPD >convert(datetime, stuff(stuff(stuff(stuff(cast(${max_data_va} as decimal), 9, 0, '' ''), 12, 0, '':''), 15, 0, '':''),18,0,''.''))'$$),
	 ('NPLG0_SEMAFORO_MENSILE','jdbc:sqlserver://pdbclt076.syssede.systest.sanpaoloimi.com\\\\SYD202:1433;DatabaseName=RDBP0_MENS;encrypt=true;trustServerCertificate=true;integratedSecurity=true;authenticationScheme=NTLM;','SYS_LG_RDB@SYSSPIMI','SECRET::SYS_LG_RDB@SYSSPIMI','com.microsoft.sqlserver.jdbc.SQLServerDriver',NULL,'SELECT ID, COD_ABI, PERIODO_RIF, TABELLA, TIPO_CARICAMENTO, PROVENIENZA, ID_FILE, COLONNA_VALORE, AMBITO, O_CARICO FROM NPLG0_SEMAFORO_MENSILE'),
	 ('NPLG0_SEMAFORO_DOMINI','jdbc:sqlserver://DBRDBP001SYS.syssede.systest.sanpaoloimi.com\\\\SYD202:1433;DatabaseName=RDBP0_DOMINI;encrypt=true;trustServerCertificate=true;integratedSecurity=true;authenticationScheme=NTLM;','SYS_LG_RDB@SYSSPIMI','SECRET::SYS_LG_RDB@SYSSPIMI','com.microsoft.sqlserver.jdbc.SQLServerDriver',NULL,'SELECT ID, TABELLA, O_CARICO FROM NPLG0_SEMAFORO_DOMINI');


drop table if exists public.tab_file_destinations;

CREATE TABLE public.tab_file_destinations (
	destination_id varchar NOT NULL,
	format_file varchar NOT NULL,
	gcs_path varchar NOT NULL,
	overwrite bool NULL,
	csv_separator varchar NULL,
	CONSTRAINT check_one_column_populated CHECK (((((format_file)::text = ANY (ARRAY[('csv'::character varying)::text, ('CSV'::character varying)::text])) AND (csv_separator IS NOT NULL)) OR (((format_file)::text = ANY (ARRAY[('parquet'::character varying)::text, ('PARQUET'::character varying)::text])) AND (csv_separator IS NULL)) OR (((format_file)::text = ANY (ARRAY[('avro'::character varying)::text, ('AVRO'::character varying)::text])) AND (csv_separator IS NULL)) OR (((format_file)::text = ANY (ARRAY[('excel'::character varying)::text, ('EXCEL'::character varying)::text])) AND (csv_separator IS NULL)))),
	CONSTRAINT chk_format_file CHECK (((format_file)::text = ANY (ARRAY[('excel'::character varying)::text, ('parquet'::character varying)::text, ('avro'::character varying)::text, ('csv'::character varying)::text, ('EXCEL'::character varying)::text, ('PARQUET'::character varying)::text, ('AVRO'::character varying)::text, ('CSV'::character varying)::text]))),
	CONSTRAINT tab_destinations_file_pk PRIMARY KEY (destination_id),
	CONSTRAINT fk_tab_file_destinations FOREIGN KEY (destination_id) REFERENCES public.tab_task_destinations(destination_id) ON DELETE CASCADE ON UPDATE CASCADE
);

GRANT SELECT ON table public.tab_file_destinations TO nplg_app;
-------------------------------------------------------------------------------------------
drop table if exists public.tab_bigquery_destinations;

CREATE TABLE public.tab_bigquery_destinations (
	destination_id varchar NOT NULL,
	project varchar NOT NULL,
	dataset varchar NOT NULL,
	tablename varchar NOT NULL,
	gcs_bucket varchar NULL,
	use_direct_write bool NULL,
	"columns" jsonb NULL,
	overwrite bool NULL,
	CONSTRAINT check_one_column_populated CHECK ((((gcs_bucket IS NULL) AND (use_direct_write = true)) OR ((gcs_bucket IS NOT NULL) AND (use_direct_write = false)))),
	CONSTRAINT tab_bigquery_destinations_pk PRIMARY KEY (destination_id),
	CONSTRAINT fk_tab_bigquery_destinations FOREIGN KEY (destination_id) REFERENCES public.tab_task_destinations(destination_id) ON DELETE CASCADE ON UPDATE CASCADE
);

GRANT SELECT ON table public.tab_bigquery_destinations TO nplg_app;

INSERT INTO public.tab_bigquery_destinations (destination_id,project,dataset,tablename,gcs_bucket,use_direct_write,"columns",overwrite) VALUES
	 ('REAGDG_STG','prj-isp-nplg0-appl-svil-001','NPLG0W','TB_REAGDG_SG',NULL,true,NULL,false),
	 ('READDR_STG','prj-isp-nplg0-appl-svil-001','NPLG0W','TB_READDR_SG',NULL,true,NULL,false),
	 ('REAGDG_SIL','prj-isp-nplg0-appl-svil-001','NPLG0W','TB_REAGDG_SIL',NULL,true,NULL,false),
	 ('READDR_SIL','prj-isp-nplg0-appl-svil-001','NPLG0W','TB_READDR_SIL',NULL,true,NULL,false);


DROP TABLE if exists public.tab_bigquery_sources;

CREATE TABLE public.tab_bigquery_sources (
	source_id varchar NOT NULL,
	project varchar NOT NULL,
	dataset varchar NOT NULL,
	tablename varchar NULL,
	query_text varchar NULL,
	CONSTRAINT check_one_column_populated CHECK ((((tablename IS NOT NULL) AND (query_text IS NULL)) OR ((tablename IS NULL) AND (query_text IS NOT NULL)))),
	CONSTRAINT tab_bigquery_sources_pk PRIMARY KEY (source_id),
	CONSTRAINT fk_tab_bigquery_sources FOREIGN KEY (source_id) REFERENCES public.tab_task_sources(source_id) ON DELETE CASCADE ON UPDATE CASCADE
);
GRANT SELECT ON table public.tab_bigquery_sources TO nplg_app;
INSERT INTO public.tab_bigquery_sources (source_id,project,dataset,tablename,query_text) VALUES
	 ('REAGDG_STG','prj-isp-nplg0-appl-svil-001','NPLG0W',NULL,'INSERT INTO <<DATASETANDTABLEDESTINATION>> select * from NPLG0W.TB_REAGDG_SG where num_banca=${cod_abi}'),
	 ('READDR_STG','prj-isp-nplg0-appl-svil-001','NPLG0W',NULL,$$'INSERT INTO <<DATASETANDTABLEDESTINATION>> select * from NPLG0W.TB_READDR_SG where num_banca=${cod_abi} and COD_PROVN_DATI_RED='${cod_provenienza}''$$);
-------------------------------------------------------------------------------------------
DROP table if exists public.tab_task_logs;

CREATE TABLE public.tab_task_logs (
	"key" jsonb NOT NULL,
	periodo int4 NULL,
	run_id varchar NOT NULL,
	state_id varchar NOT NULL,
	description varchar NULL,
	error_message varchar NULL,
	update_ts timestamp DEFAULT now() NOT NULL,
	rows_affected int4 NULL
);

GRANT SELECT, INSERT, UPDATE ON table public.tab_task_logs TO nplg_app;


DROP TABLE if exists public.tab_config_partitioning;

CREATE TABLE public.tab_config_partitioning (
	"key" jsonb NOT NULL,
	partitioning_expression varchar NULL,
	num_partitions int4 NULL,
	layer varchar DEFAULT 'stage'::character varying NOT NULL,
	CONSTRAINT tab_config_partitioning_check CHECK ((((partitioning_expression IS NOT NULL) AND (num_partitions IS NOT NULL)) OR ((partitioning_expression IS NULL) AND (num_partitions IS NULL)))),
	CONSTRAINT tab_config_partitioning_pkey PRIMARY KEY (key, layer)
);

GRANT SELECT ON table public.tab_config_partitioning TO nplg_app;

INSERT INTO public.tab_config_partitioning ("key",partitioning_expression,num_partitions,layer) VALUES
	 ('{"cod_abi": 3296, "cod_tabella": "READDR", "cod_provenienza": "DN"}','ABS(CHECKSUM(CONCAT(cod_uo,COD_NUM_PARTITA,CAST(ABS(CHECKSUM(cod_uo)) % 4 AS VARCHAR)))) % 6',6,'stage'),
	 ('{"cod_abi": 1025, "cod_tabella": "READDR", "cod_provenienza": "TC"}','ABS(CHECKSUM(CONCAT(cod_uo,COD_NUM_PARTITA,CAST(ABS(CHECKSUM(cod_uo)) % 4 AS VARCHAR)))) % 2',2,'stage'),
	 ('{"cod_abi": 1025, "cod_tabella": "REAGDG", "cod_provenienza": "AN"}',$$'(ABS(CAST(SUBSTRING(HASHBYTES('SHA1', COD_NDG),1, 4) AS int)) % 12) +1'$$,12,'stage'),
	 ('{"cod_abi": 1025, "cod_tabella": "READDR", "cod_provenienza": "V3"}','ABS(CHECKSUM(CONCAT(cod_uo,COD_NUM_PARTITA,CAST(ABS(CHECKSUM(cod_uo)) % 4 AS VARCHAR)))) % 4',4,'stage'),
	 ('{"cod_abi": 1025, "cod_tabella": "READDR", "cod_provenienza": "N1"}','ABS(CHECKSUM(CONCAT(cod_uo,COD_NUM_PARTITA,CAST(ABS(CHECKSUM(cod_uo)) % 4 AS VARCHAR)))) % 8',8,'stage'),
	 ('{"cod_abi": 3385, "cod_tabella": "READDR", "cod_provenienza": "VR"}','ABS(CHECKSUM(CONCAT(cod_uo,COD_NUM_PARTITA,CAST(ABS(CHECKSUM(cod_uo)) % 4 AS VARCHAR))))% 4',4,'stage'),
	 ('{"cod_abi": 1025, "cod_tabella": "READDR", "cod_provenienza": "CA"}','ABS(CHECKSUM(CONCAT(cod_uo,COD_NUM_PARTITA,CAST(ABS(CHECKSUM(cod_uo)) % 4 AS VARCHAR))))% 8',8,'stage'),
	 ('{"cod_abi": 1025, "cod_tabella": "READDR", "cod_provenienza": "N3"}','ABS(CHECKSUM(CONCAT(cod_uo,COD_NUM_PARTITA,CAST(ABS(CHECKSUM(cod_uo)) % 4 AS VARCHAR))))% 8',8,'stage'),
	 ('{"cod_abi": 1025, "cod_tabella": "READDR", "cod_provenienza": "VR"}','ABS(CHECKSUM(CONCAT(cod_uo,COD_NUM_PARTITA,CAST(ABS(CHECKSUM(cod_uo)) % 4 AS VARCHAR))))% 12',12,'stage'),
	 ('{"cod_abi": 1025, "cod_tabella": "READDR", "cod_provenienza": "JC"}','ABS(CHECKSUM(CONCAT(cod_uo,COD_NUM_PARTITA,CAST(ABS(CHECKSUM(cod_uo)) % 4 AS VARCHAR)))) % 2',2,'stage');

INSERT INTO public.tab_config_partitioning ("key",partitioning_expression,num_partitions,layer) VALUES
	 ('{"cod_abi": 1025, "cod_tabella": "READDR", "cod_provenienza": "TT"}','ABS(CHECKSUM(CONCAT(cod_uo,COD_NUM_PARTITA,CAST(ABS(CHECKSUM(cod_uo)) % 4 AS VARCHAR)))) % 6',6,'stage'),
	 ('{"cod_abi": 1025, "cod_tabella": "READDR", "cod_provenienza": "M1"}','ABS(CHECKSUM(CONCAT(cod_uo,
        COD_NUM_PARTITA,
        CAST(ABS(CHECKSUM(cod_uo)) % 4 AS VARCHAR)
    )
))% 6',6,'stage'),
	 ('{"cod_abi": 1025, "cod_tabella": "READDR", "cod_provenienza": "U1"}','ABS(CHECKSUM(
    CONCAT(
        cod_uo,
        COD_NUM_PARTITA,
        CAST(ABS(CHECKSUM(cod_uo)) % 4 AS VARCHAR)
    )
)) % 4',4,'stage'),
	 ('{"cod_abi": 1025, "cod_tabella": "READDR", "cod_provenienza": "TD"}','ABS(CHECKSUM(
    CONCAT(
        cod_uo,
        COD_NUM_PARTITA,
        CAST(ABS(CHECKSUM(cod_uo)) % 4 AS VARCHAR)
    )
))% 4',4,'stage'),
	 ('{"cod_abi": 3296, "cod_tabella": "READDR", "cod_provenienza": "DO"}','ABS(CHECKSUM(
    CONCAT(
        cod_uo,
        COD_NUM_PARTITA,
        CAST(ABS(CHECKSUM(cod_uo)) % 4 AS VARCHAR)
    )
)) % 4',4,'stage'),
	 ('{"cod_abi": 1025, "cod_tabella": "READDR", "cod_provenienza": "M6"}','ABS(CHECKSUM(
    CONCAT(
        cod_uo,
        COD_NUM_PARTITA,
        CAST(ABS(CHECKSUM(cod_uo)) % 4 AS VARCHAR)
    )
)) % 4',4,'stage'),
	 ('{"cod_abi": 1025, "cod_tabella": "READDR", "cod_provenienza": "TZ"}','ABS(CHECKSUM(
    CONCAT(
        cod_uo,
        COD_NUM_PARTITA,
        CAST(ABS(CHECKSUM(cod_uo)) % 4 AS VARCHAR)
    )
)) % 4',4,'stage'),
	 ('{"cod_abi": 1025, "cod_tabella": "READDR", "cod_provenienza": "V4"}','ABS(CHECKSUM(
    CONCAT(
        cod_uo,
        COD_NUM_PARTITA,
        CAST(ABS(CHECKSUM(cod_uo)) % 4 AS VARCHAR)
    )
)) % 2',2,'stage'),
	 ('{"cod_abi": 1025, "cod_tabella": "READDR", "cod_provenienza": "M2"}','ABS(CHECKSUM(
    CONCAT(
        cod_uo,
        COD_NUM_PARTITA,
        CAST(ABS(CHECKSUM(cod_uo)) % 4 AS VARCHAR)
    )
)) % 6',6,'stage'),
	 ('{"cod_abi": 1025, "cod_tabella": "READDR", "cod_provenienza": "TM"}','ABS(CHECKSUM(
    CONCAT(
        cod_uo,
        COD_NUM_PARTITA,
        CAST(ABS(CHECKSUM(cod_uo)) % 4 AS VARCHAR)
    )
)) % 2',2,'stage');
INSERT INTO public.tab_config_partitioning ("key",partitioning_expression,num_partitions,layer) VALUES
	 ('{"cod_abi": 1025, "cod_tabella": "READDR", "cod_provenienza": "06"}','ABS(CHECKSUM(
    CONCAT(
        cod_uo,
        COD_NUM_PARTITA,
        CAST(ABS(CHECKSUM(cod_uo)) % 4 AS VARCHAR)
    )
)) % 2',2,'stage'),
	 ('{"cod_abi": 1025, "cod_tabella": "READDR", "cod_provenienza": "4F"}','ABS(CHECKSUM(
    CONCAT(
        cod_uo,
        COD_NUM_PARTITA,
        CAST(ABS(CHECKSUM(cod_uo)) % 4 AS VARCHAR)
    )
)) % 2',2,'stage'),
	 ('{"cod_abi": 3296, "cod_tabella": "READDR", "cod_provenienza": "TC"}','ABS(CHECKSUM(
    CONCAT(
        cod_uo,
        COD_NUM_PARTITA,
        CAST(ABS(CHECKSUM(cod_uo)) % 4 AS VARCHAR)
    )
)) % 2',2,'stage'),
	 ('{"cod_abi": 32334, "cod_tabella": "READDR", "cod_provenienza": "P0"}','ABS(CHECKSUM(
    CONCAT(
        cod_uo,
        COD_NUM_PARTITA,
        CAST(ABS(CHECKSUM(cod_uo)) % 4 AS VARCHAR)
    )
)) % 2',2,'stage'),
	 ('{"cod_abi": 1025, "cod_tabella": "READDR", "cod_provenienza": "ET"}','ABS(CHECKSUM(
    CONCAT(
        cod_uo,
        COD_NUM_PARTITA,
        CAST(ABS(CHECKSUM(cod_uo)) % 4 AS VARCHAR)
    )
)) % 2',2,'stage'),
	 ('{"cod_abi": 3296, "cod_tabella": "READDR", "cod_provenienza": "DP"}','ABS(CHECKSUM(
    CONCAT(
        cod_uo,
        COD_NUM_PARTITA,
        CAST(ABS(CHECKSUM(cod_uo)) % 4 AS VARCHAR)
    )
))% 6',6,'stage'),
	 ('{"cod_abi": 1025, "cod_tabella": "READDR", "cod_provenienza": "T5"}','ABS(CHECKSUM(
    CONCAT(
        cod_uo,
        COD_NUM_PARTITA,
        CAST(ABS(CHECKSUM(cod_uo)) % 4 AS VARCHAR)
    )
)) % 2',2,'stage'),
	 ('{"cod_abi": 1025, "cod_tabella": "READDR", "cod_provenienza": "TF"}','ABS(CHECKSUM(
    CONCAT(
        cod_uo,
        COD_NUM_PARTITA,
        CAST(ABS(CHECKSUM(cod_uo)) % 4 AS VARCHAR)
    )
)) % 6',6,'stage'),
	 ('{"cod_abi": 3239, "cod_tabella": "READDR", "cod_provenienza": "TT"}','ABS(CHECKSUM(
    CONCAT(
        cod_uo,
        COD_NUM_PARTITA,
        CAST(ABS(CHECKSUM(cod_uo)) % 4 AS VARCHAR)
    )
)) % 2',2,'stage'),
	 ('{"cod_abi": 3296, "cod_tabella": "REAGDG", "cod_provenienza": "AN"}','(ABS(
    CAST(
      SUBSTRING(
        HASHBYTES(''SHA1'', COD_NDG),
        1, 4
      ) AS int
    )
  ) % 2) +1',2,'stage');
INSERT INTO public.tab_config_partitioning ("key",partitioning_expression,num_partitions,layer) VALUES
	 ('{"cod_abi": 1025, "cod_tabella": "READDR", "cod_provenienza": "UP"}','ABS(CHECKSUM(
    CONCAT(
        cod_uo,
       COD_NUM_PARTITA,
        CAST(ABS(CHECKSUM(cod_uo)) % 4 AS VARCHAR)
    )
)) % 2',2,'stage'),
	 ('{"cod_abi": 3385, "cod_tabella": "READDR", "cod_provenienza": "M2"}','ABS(CHECKSUM(
    CONCAT(
        cod_uo,
        COD_NUM_PARTITA,
        CAST(ABS(CHECKSUM(cod_uo)) % 4 AS VARCHAR)
    )
))% 2',2,'stage'),
	 ('{"cod_abi": 1025, "cod_tabella": "READDR", "cod_provenienza": "N2"}','ABS(CHECKSUM(
    CONCAT(
        cod_uo,
        COD_NUM_PARTITA,
        CAST(ABS(CHECKSUM(cod_uo)) % 4 AS VARCHAR)
    )
)) % 2',2,'stage'),
	 ('{"cod_abi": 1025, "cod_tabella": "READDR", "cod_provenienza": "N4"}','ABS(CHECKSUM(
    CONCAT(
        cod_uo,
        COD_NUM_PARTITA,
        CAST(ABS(CHECKSUM(cod_uo)) % 4 AS VARCHAR)
    )
)) % 2',2,'stage'),
	 ('{"cod_abi": 1025, "cod_tabella": "READDR", "cod_provenienza": "U6"}','ABS(CHECKSUM(
    CONCAT(
        cod_uo,
        COD_NUM_PARTITA,
        CAST(ABS(CHECKSUM(cod_uo)) % 4 AS VARCHAR)
    )
)) % 2',2,'stage'),
	 ('{"cod_abi": 3296, "cod_tabella": "READDR", "cod_provenienza": "N1"}','ABS(CHECKSUM(
    CONCAT(
        cod_uo,
        COD_NUM_PARTITA,
        CAST(ABS(CHECKSUM(cod_uo)) % 4 AS VARCHAR)
    )
)) % 2',2,'stage');




