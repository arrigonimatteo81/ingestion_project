from pyspark.sql import SparkSession, DataFrame

from common.const import NAME_OF_PARTITIONING_COLUMN
from common.utils import get_logger
from db.database_aware import DatabaseAware
from factories.partition_configuration_factory import PartitioningConfigurationFactory
from helpers.query_resolver import TaskContext, QueryResolver
from metadata.models.tab_jdbc import JDBCTable, JDBCQuery
from processor.sources.base import SparkReadable, NativeReadable
from processor.sources.partitioning import PartitioningConfiguration

logger = get_logger(__name__)

class TableJDBCSource(JDBCTable, DatabaseAware, SparkReadable, NativeReadable):
    def fetch_all(self, ctx):
        query_text = QueryResolver.resolve("select *, '${uid} as id_process' from " + f"{self.dbtable}", ctx)
        db = self.create_database()
        db.connect()
        res = db.fetch_all(query_text)
        db.close()
        return res

    def __init__(
            self,
            username: str,
            password: str,
            driver: str,
            url: str,
            dbtable: str,
    ):
        JDBCTable.__init__(self, username, password, driver, url, dbtable)
        #DatabaseAware.__init__(self, username, password, url)

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

class QueryJDBCSource(JDBCQuery, DatabaseAware, SparkReadable, NativeReadable):

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

        JDBCQuery.__init__(self, username, password, driver, url, query)
        self.partitioning_expression = partitioning_expression
        self.num_partitions = num_partitions


    def to_dataframe(self, spark: SparkSession, ctx: TaskContext = None) -> DataFrame:
        query_text = QueryResolver.resolve(self.query, ctx)

        if self.partitioning_expression and self.num_partitions:
            # Commentato perché, per come è adesso il partizionamento, il min e il max corrispondono a 1 e self.num_partition. È quindi inutile calcolarne il min e il max
            #pc_factory: PartitioningConfigurationFactory = PartitioningConfigurationFactory(self.create_database())
            #partitioning_cfg: PartitioningConfiguration = pc_factory.create_partitioning_configuration(self.partitioning_expression,
            #                                                                                            self.num_partitions,
            #                                                                                           query_text)
            logger.debug(
                f"Lower bound: 1, "
                f"upper bound: {self.num_partitions}"
            )
            df_reader: DataFrame = spark.read.jdbc(
                url=self.url,
                table=f"(select *, {self.partitioning_expression} as {NAME_OF_PARTITIONING_COLUMN} from ({query_text}) tab ) as subquery",
                properties={"user": self.username,"password": self.password,"driver": self.driver},
                lowerBound="1",#str(partitioning_cfg.min_value),
                upperBound=str(self.num_partitions),
                numPartitions=self.num_partitions,
                column=NAME_OF_PARTITIONING_COLUMN,
            )

        else:
            df_reader: DataFrame = spark.read.jdbc(
                url=self.url, table=f"({query_text}) as subquery", properties={"user": self.username,"password": self.password,"driver": self.driver}
            )

        return df_reader

    def fetch_all(self, ctx: TaskContext = None):
        query_text = QueryResolver.resolve(self.query, ctx)
        db = self.create_database()
        db.connect()
        res = db.fetch_all(query_text)
        db.close()
        return res
