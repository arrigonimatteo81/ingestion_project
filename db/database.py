import re
from abc import ABC, abstractmethod

from common.utils import parse_jdbc_url_string


class Database(ABC):

    def __init__(self, cfg: dict):
        self.cfg = cfg

    @abstractmethod
    def connect(self):
        pass
    @abstractmethod
    def fetch_all(self, query: str, params=None):
        pass
    @abstractmethod
    def close(self):
        pass

class DbConcrete(Database):

    pattern=""

    def __init__(self, cfg: dict):
        super().__init__(cfg)
        self.conn = None
        self.cursor = None

    def match_url(self):
        match = parse_jdbc_url_string(self.cfg["url"],self.pattern)
        return match

    def fetch_all(self, query: str, params=None):
        self.cursor.execute(query, params or ())
        return self.cursor.fetchall()

    def execute(self,query, params=None):
        self.cursor.execute(query, params or ())
        return self.cursor.rowcount

    def execute_many(self, query: str, params=None):
        cur = self.conn.cursor()
        try:
            cur.executemany(query, params)
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        finally:
            cur.close()

    def close(self):
        self.cursor.close()
        self.conn.close()

    def connect(self):
        pass

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False