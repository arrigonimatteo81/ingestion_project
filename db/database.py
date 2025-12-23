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
    def execute(self, query: str, params=None):
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

    def execute(self, query: str, params=None):
        self.cursor.execute(query, params or ())
        return self.cursor.fetchall()

    def close(self):
        self.cursor.close()
        self.conn.close()

    def connect(self):
        pass