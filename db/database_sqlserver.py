import pymssql

from db.database import DbConcrete


class SqlServerDB(DbConcrete):

    pattern = r"^jdbc:sqlserver://(?P<host>[^\\:;]+)(?:\\(?P<instance>[^:;]+))?(?::(?P<port>\d+))?.*?databaseName=(?P<db>[^;]+)"

    def connect(self):
        match = self.match_url()
        server = f"{match.group('host')}\\{match.group('instance')}"
        self.conn=pymssql.connect(
                    server=match.group('host'),
                    user=self._switch_user_components(),
                    password=self.cfg["password"],
                    database=match.group("db")
            )
        self.cursor = self.conn.cursor(as_dict=True) #as_dict=True valido solo per pymssql: torna i nomi delle colonne della query eseguita
        return self

    def _switch_user_components(self):
        return f"{self.cfg['user'].split('@')[1]}\\{self.cfg['user'].split('@')[0]}"
