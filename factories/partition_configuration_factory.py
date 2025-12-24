from factories.database_factory import DatabaseFactory
from helper.partition_bounds_resolver import PartitionBoundsResolver
from processor.sources.partitioning import PartitioningConfiguration


class PartitioningConfigurationFactory:
    def __init__(self, db_factory: DatabaseFactory):
        self.db_factory = db_factory

    def create_partitioning_configuration(self, expression, num_partitions, query):
        db = self.db_factory.get_db()

        min_v, max_v = PartitionBoundsResolver().resolve(
            expression, query, db
        )

        return PartitioningConfiguration(
            expression=expression,
            num_partitions=num_partitions,
            min_value=min_v,
            max_value=max_v
        )