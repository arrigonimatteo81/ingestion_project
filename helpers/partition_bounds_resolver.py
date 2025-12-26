from db.database import DbConcrete


class PartitionBoundsResolver:

    @staticmethod
    def resolve(expression: str, query: str, db: DbConcrete) -> tuple[int, int]:
        sql = f"SELECT coalesce(MIN({expression}),0), coalesce(MAX({expression}),99999) FROM ({query}) t"
        db.connect()
        res = db.execute(sql)[0]
        db.close()
        return res