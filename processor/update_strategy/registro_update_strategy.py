from abc import ABC, abstractmethod
from dataclasses import dataclass

from helpers.query_resolver import TaskContext
from processor.domain import Metric


@dataclass
class ExecutionResult:
    max_date: int = None

class RegistroUpdateStrategy(ABC):

    @abstractmethod
    def update(self, er: ExecutionResult, ctx: TaskContext):
        pass

    def required_metrics(self) -> Metric:
        pass


class IdAndDateUpdateStrategy(RegistroUpdateStrategy):

    def required_metrics(self):
        return Metric.MAX_DATA_VA

    def update(self, er: ExecutionResult, ctx: TaskContext):

        max_data = er.max_date

        ctx.registro_repo.upsert(
            chiave=ctx.key,
            last_id=ctx.task.query_params.get("id"),
            max_data_va=max_data
        )


class OnlyIdUpdateStrategy(RegistroUpdateStrategy):

    def update(self, er: ExecutionResult, ctx: TaskContext):
        ctx.registro_repo.upsert(
            chiave=ctx.key,
            last_id=ctx.task.query_params.get("id")
        )

class NoOpRegistroUpdateStrategy(RegistroUpdateStrategy):

    def update(self, er, ctx):
        # intenzionalmente vuoto
        return