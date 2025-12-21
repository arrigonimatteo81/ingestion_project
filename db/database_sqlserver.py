import pyodbc
from db.database import Database, DbConcrete


class SqlServerDB(DbConcrete):

    pattern = (
        r"^jdbc:sqlserver://"
        r"(?P<host>[^\\:;]+)"  # host
        r"(?:\\(?P<instance>[^:;]+))?"  # istanza (opzionale)
        r"(?::(?P<port>\d+))?"  # porta (opzionale)
        r".*?databaseName=(?P<db>[^;]+)"
    )

    def connect(self):
        match = self.match_url()
        server = f"{match.group('host')}\\{match.group('instance')}"
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={server};"
            f"DATABASE={match.group('db')};"
            f"UID={self.cfg['user']};"
            f"PWD={self.cfg['password']}"
        )
        self.conn = pyodbc.connect(conn_str)
        self.cursor = self.conn.cursor()
        return self