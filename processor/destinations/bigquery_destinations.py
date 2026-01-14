from google.cloud import bigquery
from pyspark.sql import DataFrame, DataFrameWriter

from common.const import TABLEPLACEHOLDER
from common.utils import get_logger
from metadata.models.tab_bigquery import BigQuery
from processor.destinations.base import Destination, SparkWritable, BigQueryWritable, NativeWritable

logger = get_logger(__name__)

class TableBigQueryDestination(SparkWritable,BigQueryWritable, BigQuery, Destination, NativeWritable):



    def __init__(
            self,
            project: str,
            dataset: str,
            db_table_destination: str,
            overwrite: bool,
            columns: list[str]
    ):
        Destination.__init__(self, overwrite)
        BigQuery.__init__(self, project, dataset)
        self.db_table_destination = db_table_destination
        self.client_bigquery = bigquery.Client()
        self.columns = columns

    def write(self, df: DataFrame):
        if self.overwrite:
            mode_write = "overwrite"
        else:
            mode_write = "append"

        tmp_df_writer: DataFrameWriter = (
            df.write.format(self.format)
            .option("table", f"{self.project}.{self.dataset}.{self.db_table_destination}")
            .option("writeMethod", "direct")
            .mode(mode_write)
        )

        #if self.partition_columns_list:
        #    tmp_df_writer.partitionBy(*self.partition_columns_list)

        #if not self.use_direct_write:
        #    tmp_df_writer.option("temporaryGcsBucket", self.gcs_bucket_name)
        #else:
        #    tmp_df_writer.option("writeMethod", "direct")

        logger.debug(f"tmp_df_writer.save...")
        tmp_df_writer.save()

    def write_query(self,query, ctx):

        destination = self.resolve_destination()
        
        if self.overwrite:
            truncate_sql = f"TRUNCATE TABLE {destination}"
            self.client_bigquery.query(truncate_sql).result()

        if TABLEPLACEHOLDER not in query:
            raise ValueError("Destination placeholder not found")

        final_query = query.replace(TABLEPLACEHOLDER, f"{destination}")

        job = self.client_bigquery.query(final_query)
        job.result()

    def write_rows(self, rows):
        table_id = self.resolve_destination()

        if self.overwrite:
            self.client_bigquery.query(f"TRUNCATE TABLE {table_id}").result()

        batch = []
        for row in rows:
            batch.append(dict(zip(self.columns, row)))
            if len(batch) >= 500:
                self.client_bigquery.insert_rows_json(table_id, batch)
                batch.clear()

        if batch:
            self.client_bigquery.insert_rows_json(table_id, batch)

    def resolve_destination(self) -> str:
        return f"`{self.project}.{self.dataset}.{self.db_table_destination}`"

