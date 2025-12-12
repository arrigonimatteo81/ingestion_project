from dataclasses import dataclass


@dataclass
class OperationResult:
    successful: bool
    description: str