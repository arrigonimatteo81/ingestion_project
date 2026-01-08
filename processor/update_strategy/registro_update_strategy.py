from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, functions as F

from helpers.query_resolver import TaskContext


class RegistroUpdateStrategy(ABC):

    @abstractmethod
    def update(self, df: DataFrame, ctx: TaskContext):
        pass


class IdAndDateUpdateStrategy(RegistroUpdateStrategy):

    def update(self, df: DataFrame, ctx: TaskContext):
        max_data = df.agg(
            F.max("num_data_va").alias("max_data")
        ).collect()[0]["max_data"]

        ctx.registro_repo.upsert(
            chiave=ctx.key,
            last_id=ctx.task.query_params.get("id"),
            max_data_va=max_data
        )


class OnlyIdUpdateStrategy(RegistroUpdateStrategy):

    def update(self, df: DataFrame, ctx: TaskContext):
        ctx.registro_repo.upsert(
            chiave=ctx.key,
            last_id=ctx.task.query_params.get("id")
        )

class NoOpRegistroUpdateStrategy(RegistroUpdateStrategy):
    def update(self, df, ctx):
        # intenzionalmente vuoto
        return

"""class DominiUpdateStrategy(RegistroUpdateStrategy):

    def update(self, df: DataFrame, ctx: TaskContext):
        ctx.registro_repo.upsert(
            chiave={"tabella": ctx.task.tabella},
            last_id=ctx.task.query_params.get("id")
        )"""