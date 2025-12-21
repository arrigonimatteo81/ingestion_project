import psycopg2
from db.database import DbConcrete


class PostgresDB(DbConcrete):

    pattern = r"^jdbc:postgresql://(?P<host>[^:/]+):(?P<port>\d+)/(?P<db>[^/?]+)"

    def connect(self):
        match = self.match_url()

        self.conn = psycopg2.connect(
            host=match.group("host"),
            port=match.group("port"),
            dbname=match.group("db"),
            user=self.cfg["user"],
            password=self.cfg["password"]
    )
        self.cursor = self.conn.cursor()
        return self
