from dataclasses import dataclass

import psycopg
import json
from metadata.models.tab_conn import TabConn
from metadata.models.tab_dest import TabDest
from metadata.models.tab_src import TabSrc


class MetadataLoader:

    def __init__(self, meta_db_conn):
        self.conn = psycopg.connect(**meta_db_conn)

@dataclass
class TabConfig:
    config_name: str
    config_value: str
    def __str__(self):
        return f"TabConfig(config_name={self.config_name},config_value={self.config_name})"

class CommonMetadata(MetadataLoader):
    def __init__(self, meta_db_conn):
        super().__init__(meta_db_conn)


    def get(self, config_name):
        cur = self.conn.cursor()
        cur.execute(f"SELECT config_name, config_value FROM public.tab_configurations where config_name={config_name}")
        row = cur.fetchfirst()
        return TabConfig(*row[:-1])

    def get_all(self) -> [TabConfig]:
        cur = self.conn.cursor()
        cur.execute(f"SELECT config_name, config_value FROM public.tab_configurations")
        rows = cur.fetchall()
        result = []
        for r in rows:
            result.append(TabConfig(*r[:-1]))
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
