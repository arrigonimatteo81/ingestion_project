from pyspark.sql import DataFrame, SparkSession

from helpers.query_resolver import QueryResolver, TaskContext
from metadata.models.tab_bigquery import BigQueryTable, BigQueryQuery
from processor.sources.base import SparkReadable, BigQueryReadable


class QueryBigQuerySource(BigQueryQuery, SparkReadable, BigQueryReadable):
    def to_query(self, ctx) -> str:
        return QueryResolver.resolve(self.query, ctx)

    def to_dataframe(self, spark: SparkSession, ctx: TaskContext):
        spark.conf.set("parentProject", self.project)
        df_reader: DataFrame = (
            spark.read.format(self.format)
            .option("query", self.to_query(ctx))
            .option("viewsEnabled", "true")
            .option("materializationDataset", self.dataset)
        ).load()
        return df_reader

    def __init__(self, project: str, dataset: str, query: str ):#spark_read_options: dict = None
    #):
        BigQueryQuery.__init__(self, project, dataset, query)

class TableBigQuerySource(BigQueryTable, SparkReadable, BigQueryReadable):
    def to_query(self, ctx) -> str:
        return QueryResolver.resolve("select *, '${uid}' as id_process from " + f"{self.project}.{self.dataset}.{self.table}", ctx)

    def to_dataframe(self, spark, ctx):
        spark.conf.set("parentProject", self.project)
        df_reader: DataFrame = (
            spark.read.format(self.format)
            .option("project", self.project)
            .option("dataset", f" {self.project}.{self.dataset}")
            .option("table", f"{self.project}.{self.dataset}.{self.table}")
        ).load()
        return df_reader

    def __init__(self, project: str, dataset: str, table: str ):

        BigQueryTable.__init__(self, project, dataset, table)