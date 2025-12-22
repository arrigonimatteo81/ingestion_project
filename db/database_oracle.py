import cx_Oracle
from db.database import DbConcrete


class OracleDB(DbConcrete):

    pattern = r"HOST=(?P<host>[^)]+).*?PORT=(?P<port>\d+).*?(SERVICE_NAME|SID)=(?P<db>[^)]+)"

    def connect(self):
        match = self.match_url()
        self.conn = cx_Oracle.connect(
            user=self.cfg["user"],
            password=self.cfg["password"],
            dsn=self.cfg["dsn"]
        )
        self.cursor = self.conn.cursor()
        return self
