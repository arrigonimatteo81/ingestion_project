from db.database import DbConcrete
from factories.database_factory import DatabaseFactory
from helpers.partition_bounds_resolver import PartitionBoundsResolver
from processor.sources.partitioning import PartitioningConfiguration


class PartitioningConfigurationFactory:
    def __init__(self, db: DbConcrete):
        self.db = db

    def create_partitioning_configuration(self, expression, num_partitions, query):
        min_v, max_v = PartitionBoundsResolver().resolve(
            expression, query, self.db
        )

        return PartitioningConfiguration(
            expression=expression,
            num_partitions=num_partitions,
            min_value=min_v,
            max_value=max_v
        )