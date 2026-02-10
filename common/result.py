from dataclasses import dataclass
from typing import Any, Dict, Optional

@dataclass
class OperationResult:
    successful: bool
    description: str

class ExecutionResult:

    def __init__(self, metrics: Optional[Dict[str, Any]] = None):
        self._metrics: Dict[str, Any] = metrics or {}

    def get(self, key: str, default=None):
        return self._metrics.get(key, default)

    def set(self, key: str, value: Any):
        self._metrics[key] = value

    def has(self, key: str) -> bool:
        return key in self._metrics

    @property
    def metrics(self) -> Dict[str, Any]:
        return self._metrics

    def __repr__(self):
        return f"ExecutionResult({self._metrics})"