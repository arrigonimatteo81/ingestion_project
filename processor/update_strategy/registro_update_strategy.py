from abc import ABC, abstractmethod
from dataclasses import dataclass

from helpers.query_resolver import TaskContext
from metadata.loader.metadata_loader import RegistroRepository
from processor.domain import Metric


@dataclass
class ExecutionResult:
    max_date: int = None

class RegistroUpdateStrategy(ABC):

    @abstractmethod
    def update(self, er: ExecutionResult, ctx: TaskContext, repo: RegistroRepository):
        pass

    def required_metrics(self) -> Metric:
        pass


class IdAndDateUpdateStrategy(RegistroUpdateStrategy):

    def required_metrics(self):
        return Metric.MAX_DATA_VA

    def update(self, er: ExecutionResult, ctx: TaskContext, repo: RegistroRepository):

        max_data = er.max_date

        repo.upsert(
            chiave=ctx.key,
            last_id=ctx.task.query_params.get("id"),
            max_data_va=max_data,
            periodo=ctx.task.query_params.get("num_periodo_rif")
        )


class OnlyIdUpdateStrategy(RegistroUpdateStrategy):

    def update(self, er: ExecutionResult, ctx: TaskContext, repo: RegistroRepository):
        repo.upsert(
            chiave=ctx.key,
            last_id=ctx.task.query_params.get("id")
        )


class FileUpdateStrategy(RegistroUpdateStrategy):

    def update(self, er: ExecutionResult, ctx: TaskContext, repo: RegistroRepository):
        repo.insert_rett(
            chiave=ctx.key,
            files=ctx.task.query_params.get("id_files"),
            run_id=ctx.run_id,
            process_id=ctx.task.uid
        )

class NoOpRegistroUpdateStrategy(RegistroUpdateStrategy):

    def update(self, er, ctx, repo):
        # intenzionalmente vuoto
        return