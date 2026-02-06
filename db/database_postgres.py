import psycopg2
import psycopg2.extras
from db.database import DbConcrete


class PostgresDB(DbConcrete):

    pattern = r"^jdbc:postgresql://(?P<host>[^:/]+):(?P<port>\d+)/(?P<db>[^/?]+)"


    def connect(self):
        match = self.match_url()

        self.conn = psycopg2.connect(
            host=match.group("host"),
            port = int(match.group("port")) if match.group("port") is not None else 5432,
            dbname=match.group("db"),
            user=self.cfg["user"],
            password=self.cfg["password"]
    )
        self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        return self
