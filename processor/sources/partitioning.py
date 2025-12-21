from dataclasses import dataclass

from db.database_factory import DatabaseFactory

MAX_ALLOWED_PARTITIONS = 300


class PartitioningConfiguration:
    """
    Configuration for data partitioning.

    Attributes:
        expression (str): The expression used for partitioning. It can also be the name of an existing column. In both cases, only integer expressions are allower
        num_partitions (int): The number of partitions to create.
    """
    def __init__(self, expression: str, num_partitions: int, username: str, pwd: str, url: str, query: str):
        self.expression = expression
        self.num_partitions = num_partitions
        self.username = username
        self.pwd = pwd
        self.url = url
        self.query = query
        self._min: int
        self._max: int

    @property
    def min(self):
        return self._min

    @property
    def max(self):
        return self._max

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

        _db = DatabaseFactory.get_db({"user": self.username, "password": self.pwd, "url": self.url})
        res = _db.execute(f"Select min{self.expression}, max{self.expression} from ({self.query})")
        self._min = res(0)
        self._max = res(1)

