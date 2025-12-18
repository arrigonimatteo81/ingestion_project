from pyspark.sql import SparkSession, DataFrame

from metadata.models.tab_jdbc import JDBCTable, JDBCQuery
from processor.sources.partitioning import PartitioningConfiguration
from processor.sources.base import Source


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
        #TODO aggiungere gestione partizionamento
        df_reader = (
            spark.read.format(self.format)
            .option("url", self.url)
            .option("dbtable", self.query)
            .option("user", self.username)
            .option("password", self.password)
            .option("driver", self.driver)
        )
        return df_reader.load()

