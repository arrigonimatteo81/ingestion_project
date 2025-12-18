from dataclasses import dataclass

MAX_ALLOWED_PARTITIONS = 300


@dataclass
class PartitioningConfiguration:
    """
    Configuration for data partitioning.

    Attributes:
        expression (str): The expression used for partitioning. It can also be the name of an existing column. In both cases, only integer expressions are allower
        num_partitions (int): The number of partitions to create.
    """

    # column_name: str
    expression: str
    num_partitions: int

    def __post_init__(self):
        """
        Validates the initialization parameters after the dataclass is created.

        Raises:
            ValueError: If any of the validation checks fail.
        """

        # Ensure the partitioning expression is not empty
        if not self.expression:
            raise ValueError("partitioning_expression cannot be empty")

        # Ensure the number of partitions is a positive integer within the allowed limit
        if (
                not isinstance(self.num_partitions, int)
                or self.num_partitions <= 0
                or self.num_partitions > MAX_ALLOWED_PARTITIONS
        ):
            raise ValueError(
                f"num_partitions must be a positive integer not bigger than {MAX_ALLOWED_PARTITIONS}"
            )
