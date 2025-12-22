import re

import mysql.connector
from mysql.connector import Error
from db.database import  DbConcrete


class MySqlDB(DbConcrete):

    pattern = r"^jdbc:mysql://(?P<host>[^:/]+)(?::(?P<port>\d+))?/(?P<db>[^/?]+)"

    def connect(self):
        try:

            match = self.match_url()

            self.conn = mysql.connector.connect(
                host=match.group("host"),
                port = int(match.group("port")) if match.group("port") is not None else 3306,
                database=match.group("db"),
                user=self.cfg["user"],
                password=self.cfg["password"],
                autocommit=False
            )
            self.cursor = self.conn.cursor()
            return self
        except Error as e:
            raise RuntimeError(f"MySQL connection error: {e}")
