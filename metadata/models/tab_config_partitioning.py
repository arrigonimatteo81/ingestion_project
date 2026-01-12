from dataclasses import dataclass

@dataclass
class TabConfigPartitioning:
    partitioning_expression: str = None
    num_partitions: int = None

    def __repr__(self):
        return (f"TabConfigPartitioning(partitioning_expression={self.partitioning_expression},"
                f"num_partitions={self.num_partitions})")