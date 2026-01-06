from pyspark.sql import SparkSession, DataFrame

from common.const import NAME_OF_PARTITIONING_COLUMN
from common.utils import get_logger
from factories.database_factory import DatabaseFactory
from factories.partition_configuration_factory import PartitioningConfigurationFactory
from helpers.query_resolver import TaskContext, QueryResolver
from metadata.models.tab_jdbc import JDBCTable, JDBCQuery
from processor.sources.partitioning import PartitioningConfiguration
from processor.sources.base import Source

logger = get_logger(__name__)

class TableJDBCSource(Source, JDBCTable):
    def __init__(
            self,
            username: str,
            password: str,
            driver: str,
            url: str,
            dbtable: str,
    ):
        Source.__init__(self)
        JDBCTable.__init__(self, username, password, driver, url, dbtable)

    def to_dataframe(self, spark: SparkSession, ctx: TaskContext = None) -> DataFrame:
        df_reader = (
            spark.read.format(self.format)
            .option("url", self.url)
            .option("dbtable", self.dbtable)
            .option("user", self.username)
            .option("password", self.password)
            .option("driver", self.driver)
        )
        return df_reader.load()

class QueryJDBCSource(Source, JDBCQuery):

    def __init__(
            self,
            username: str,
            password: str,
            driver: str,
            url: str,
            query: str,
            partitioning_expression: str = None,
            num_partitions: int = None
    ):

        Source.__init__(self)
        JDBCQuery.__init__(self, username, password, driver, url, query)
        self.partitioning_expression = partitioning_expression
        self.num_partitions = num_partitions


    def to_dataframe(self, spark: SparkSession, ctx: TaskContext = None) -> DataFrame:
        query_text = QueryResolver.resolve(self.query, ctx)

        if self.partitioning_expression and self.num_partitions:
            #print(f"MATTEO2: (select *, {self.partitioning_expression} as {NAME_OF_PARTITIONING_COLUMN} from ({query_text}) tab ) as subquery")
            db_factory: DatabaseFactory = DatabaseFactory({"user": self.username, "password": self.password, "url": self.url})
            pc_factory: PartitioningConfigurationFactory = PartitioningConfigurationFactory(db_factory)
            partitioning_cfg: PartitioningConfiguration = pc_factory.create_partitioning_configuration(self.partitioning_expression,
                                                                                                       self.num_partitions,
                                                                                                       query_text)
            logger.debug(
                f"Lower bound: {partitioning_cfg.min_value}, "
                f"upper bound: {partitioning_cfg.max_value}"
            )
            df_reader: DataFrame = spark.read.jdbc(
                url=self.url,
                table=f"(select *, {self.partitioning_expression} as {NAME_OF_PARTITIONING_COLUMN} from ({query_text}) tab ) as subquery",
                properties={"user": self.username,"password": self.password,"driver": self.driver},
                lowerBound=str(partitioning_cfg.min_value),
                upperBound=str(partitioning_cfg.max_value),
                numPartitions=partitioning_cfg.num_partitions,
                column=NAME_OF_PARTITIONING_COLUMN,
            )

        else:
            df_reader: DataFrame = spark.read.jdbc(
                url=self.url, table=f"({query_text}) as subquery", properties={"user": self.username,"password": self.password,"driver": self.driver}
            )

        return df_reader

