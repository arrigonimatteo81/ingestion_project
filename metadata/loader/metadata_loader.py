from dataclasses import dataclass

import psycopg2
import json

from common.utils import get_logger
from metadata.models.tab_conn import TabConn
from metadata.models.tab_dest import TabDest
from metadata.models.tab_src import TabSrc

logger = get_logger(__name__)

@dataclass
class TabConfig:
    config_name: str
    config_value: str
    def __str__(self):
        return f"TabConfig(config_name={self.config_name},config_value={self.config_name})"

@dataclass
class TabGroups:
    task_id: str
    group_name: str
    def __str__(self):
        return f"TabGroups(task_id={self.task_id},group_name={self.group_name})"

class MetadataLoader:

    def __init__(self, meta_db_conn):
        self.conn = psycopg2.connect(**meta_db_conn)

class CommonMetadata(MetadataLoader):

    def get(self, config_name):
        cur = self.conn.cursor()
        cur.execute(f"SELECT config_name, config_value FROM public.tab_configurations where config_name={config_name}")
        row = cur.fetchone()
        return TabConfig(*row[:-1])

    def get_all(self) -> [TabConfig]:
        cur = self.conn.cursor()
        cur.execute(f"SELECT config_name, config_value FROM public.tab_configurations")
        rows = cur.fetchall()
        result = []
        for r in rows:
            result.append(TabConfig(*r[:-1]))
        return result

class OrchestratorMetadata(CommonMetadata):

    def get_all_tasks_in_group(self, groups: [str]) -> [TabGroups]:
        cur = self.conn.cursor()
        str_group = "',".join(groups)
        cur.execute(f"SELECT * FROM public.task_group where group_name in ('{str_group}')")
        rows = cur.fetchall()
        result = []
        for r in rows:
            result.append(TabGroups(*r[:-1]))

        return result





    """def load_connections(self):
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM tab_conn")
        rows = cur.fetchall()
        result = {}
        for r in rows:
            result[r[0]] = TabConn(*r[:-1], extra_params=json.loads(r[-1]) if r[-1] else {}).__dict__
        return result

    def load_destinations(self):
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM tab_dest")
        rows = cur.fetchall()
        result = {}
        for r in rows:
            result[r[0]] = TabDest(*r[:-1], extra_params=json.loads(r[-1]) if r[-1] else {}).__dict__
        return result

    def load_sources(self):
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM tab_src")
        rows = cur.fetchall()
        result = []
        for r in rows:
            result.append(TabSrc(*r).__dict__)
        return result"""
