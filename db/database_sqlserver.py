import pymssql
from db.database import Database, DbConcrete


class SqlServerDB(DbConcrete):

    pattern = r"^jdbc:sqlserver://(?P<host>[^\\:;]+)(?:\\(?P<instance>[^:;]+))?(?::(?P<port>\d+))?.*?databaseName=(?P<db>[^;]+)"

    def connect(self):
        match = self.match_url()
        server = f"{match.group('host')}\\{match.group('instance')}"
        self.conn=pymssql.connect(
                    server=server,
                    user=self.cfg["user"],
                    password=self.cfg["password"],
                    database=match.group("db")
            )
        self.cursor = self.conn.cursor()
        return self

#with pymssql.connect(server=r"pdbclt076.syssede.systest.sanpaoloimi.com",user=r"SYSSPIMI\SYS_LG_RDB",password="4EfTw@B9UpCriK#epGiM",database="RDBP0_MENS") as db_source:
#...     with db_source.cursor() as cursor:
#...             cursor.execute("Select 1")
#...             cursor.fetchall()