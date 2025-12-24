from pyspark.sql import SparkSession, DataFrame

from common.const import NAME_OF_PARTITIONING_COLUMN
from common.utils import get_logger
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

    def to_dataframe(self, spark: SparkSession) -> DataFrame:
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
            partitioning_configuration: PartitioningConfiguration = None,
    ):

        Source.__init__(self)
        JDBCQuery.__init__(self, username, password, driver, url, query)
        self._partitioning_configuration = partitioning_configuration

    def to_dataframe(self, spark: SparkSession) -> DataFrame:
        if self._partitioning_configuration:
            logger.debug(
                f"Lower bound: {self._partitioning_configuration.min_value}, "
                f"upper bound: {self._partitioning_configuration.max_value}"
            )
            df_reader: DataFrame = spark.read.jdbc(
                url=self.url,
                table=self.query,
                properties={"user": self.username,"password": self.password,"driver": self.driver},
                lowerBound=str(self._partitioning_configuration.min_value),
                upperBound=str(self._partitioning_configuration.max_value),
                numPartitions=self._partitioning_configuration.num_partitions,
                column=NAME_OF_PARTITIONING_COLUMN,
            )

        else:
            df_reader: DataFrame = spark.read.jdbc(
                url=self.url, table=self.query, properties={"user": self.username,"password": self.password,"driver": self.driver}
            )

        return df_reader

