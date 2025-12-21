import cx_Oracle
from db.database import Database

class OracleDB(Database):

    def connect(self):
        self.conn = cx_Oracle.connect(
            user=self.cfg["user"],
            password=self.cfg["password"],
            dsn=self.cfg["dsn"]
        )
        self.cursor = self.conn.cursor()
        return self

    def execute(self, query: str, params=None):
        self.cursor.execute(query, params or {})
        return self.cursor.fetchall()

    def close(self):
        self.cursor.close()
        self.conn.close()
