import psycopg
import json
from metadata.models.tab_conn import TabConn
from metadata.models.tab_dest import TabDest
from metadata.models.tab_src import TabSrc


class MetadataLoader:

    def __init__(self, meta_db_conn):
        self.conn = psycopg.connect(**meta_db_conn)

    def load_connections(self):
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
        return result
