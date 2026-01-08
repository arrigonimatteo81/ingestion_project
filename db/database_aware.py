from abc import ABC

from db.database import DbConcrete
from factories.database_factory import DatabaseFactory


class DatabaseAware(ABC):

    def create_database(self) -> DbConcrete:
        cfg = {
            "user": self.username,
            "password": self.password,
            "url": self.url
        }
        return DatabaseFactory(cfg).get_db()