from common.const import NAME_OF_PARTITIONING_COLUMN
from db.database import DbConcrete


class PartitionBoundsResolver:

    @staticmethod
    def resolve(expression: str, query: str, db: DbConcrete) -> tuple[int, int]:
        query_for_partitioning: str = f"select *, {expression} as {NAME_OF_PARTITIONING_COLUMN} from ({query}) tab"
        sql = f"SELECT coalesce(MIN({NAME_OF_PARTITIONING_COLUMN}),0), coalesce(MAX({NAME_OF_PARTITIONING_COLUMN}),99999) FROM ({query_for_partitioning}) t"
        db.connect()
        res = db.fetch_all(sql)[0]
        db.close()
        return res