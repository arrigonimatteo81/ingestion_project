from dataclasses import dataclass

@dataclass
class TabJDBCSource:
    url: str
    username: str
    pwd: str
    driver: str
    tablename: str
    query_text: str = None
    partitioning_expression: str = None
    num_partitions: int = None

    def __repr__(self):
        return (f"TabJDBCSource(url={self.url},username={self.username},pwd={self.pwd},driver={self.driver},"
                f"tablename={self.tablename}, query_text={self.query_text},partitioning_expression={self.partitioning_expression},"
                f"num_partitions={self.num_partitions})")

@dataclass
class TabJDBCDest:
    url: str
    username: str
    pwd: str
    driver: str
    tablename: str
    overwrite: bool = False

    def __repr__(self):
        return (f"TabJDBCDest(url={self.url},username={self.username},pwd={self.pwd},driver={self.driver},"
                f"tablename={self.tablename}, overwrite={self.overwrite})")

class JDBC:

    format = "jdbc"

    def __init__(self, username: str, password: str, driver: str, url: str):
        self._username = username
        self._password = password
        self._driver = driver
        self._url = url

    @property
    def username(self):
        return self._username

    @property
    def password(self):
        return self._password

    @property
    def url(self):
        return self._url

    @property
    def driver(self):
        return self._driver

class JDBCTable(JDBC):
    def __init__(
            self,
            username: str,
            password: str,
            driver: str,
            url: str,
            dbtable: str,
    ):
        """
        Initializes a new instance of the JDBCTable class.

        Args:
            username (str): The username for the database connection.
            password (str): The password for the database connection.
            driver (str): The JDBC driver to use for the connection.
            url (str): The database URL.
            dbtable (str): The name of the database table to interact with.
        """
        # Initialize the base JDBC class with the provided credentials and connection details.
        JDBC.__init__(self, username, password, driver, url)
        # Set the specific database table for this instance.
        self.dbtable = dbtable

class JDBCQuery(JDBC):
    def __init__(
            self,
            username: str,
            password: str,
            driver: str,
            url: str,
            query: str,
    ):
        """
        Initializes a new instance of the JDBCTable class.

        Args:
            username (str): The username for the database connection.
            password (str): The password for the database connection.
            driver (str): The JDBC driver to use for the connection.
            url (str): The database URL.
            dbtable (str): The name of the database table to interact with.
        """
        # Initialize the base JDBC class with the provided credentials and connection details.
        JDBC.__init__(self, username, password, driver, url)
        # Set the specific database table for this instance.
        self.query = query