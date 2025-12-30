import json
from dataclasses import dataclass, asdict
from typing import Optional

from metadata.models.tab_tasks import TaskSemaforo


@dataclass
class TaskSemaforoPayload:
    uid: Optional[str] = None
    id: Optional[int] = None
    cod_abi: Optional[int] = None
    source_id: Optional[str] = None
    destination_id: Optional[str] = None
    cod_provenienza: Optional[str] = None
    num_periodo_rif: Optional[int] = None
    cod_gruppo: Optional[str] = None
    cod_colonna_valore: Optional[str] = None
    num_ambito: Optional[int] = None
    num_max_data_va: Optional[int] = None

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, json_str: str) -> "TaskSemaforoPayload":
        data = json.loads(json_str)
        return cls(**data)

    def to_domain(self) -> TaskSemaforo:
        return TaskSemaforo(
            uid=self.uid or "",
            id=self.id or 0,
            cod_abi=self.cod_abi or 0,
            source_id=self.source_id or "",
            destination_id=self.destination_id or "",
            cod_provenienza=self.cod_provenienza or "",
            num_periodo_rif=self.num_periodo_rif or 0,
            cod_gruppo=self.cod_gruppo or "",
            cod_colonna_valore=self.cod_colonna_valore or "",
            num_ambito=self.num_ambito or 0,
            num_max_data_va=self.num_max_data_va or 0
        )