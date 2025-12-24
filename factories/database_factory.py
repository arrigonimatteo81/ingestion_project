from common.utils import get_logger, extract_db_type_from_jdbc_url
#from db.database_mysql import MySqlDB
#from db.database_oracle import OracleDB
from db.database_postgres import PostgresDB
from db.database_sqlserver import SqlServerDB

#from db.database_sqlserver import SqlServerDB

logger = get_logger(__name__)

class DatabaseFactory:

    def __init__(self, cfg):
        self.cfg = cfg

    def get_db(self):
        db_type = extract_db_type_from_jdbc_url(self.cfg["url"])
        try:
            #if db_type.upper() == "ORACLE":
            #    return OracleDB(cfg)
            if db_type.upper() == "POSTGRESQL":
                return PostgresDB(self.cfg)
            #elif db_type.upper() == "MYSQL":
            #    return MySqlDB(cfg)
            elif db_type.upper() == "SQLSERVER":
                return SqlServerDB(self.cfg)
            else:
                logger.error("Unsupported db type!!!")
                raise ValueError(f"Unsupported db type: {db_type.upper()}")
        except Exception as exc:
            logger.error(f"Failed to create db: {exc}")
            raise
