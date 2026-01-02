from dataclasses import dataclass, asdict
from typing import Dict, Optional
import json

from metadata.models.tab_tasks import TaskSemaforo


@dataclass
class TaskSemaforoPayload:
    # identità tecnica
    uid: str

    # configurazione tecnica
    source_id: str
    destination_id: str
    tipo_caricamento: str

    # business
    key: Dict
    query_params: Dict

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, json_str: str) -> "TaskSemaforoPayload":
        return cls(**json.loads(json_str))

    def to_domain(self) -> "TaskSemaforo":
        return TaskSemaforo(
            uid=self.uid,
            source_id=self.source_id,
            destination_id=self.destination_id,
            tipo_caricamento=self.tipo_caricamento,
            key=self.key,
            query_params=self.query_params
        )
