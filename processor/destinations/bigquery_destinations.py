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
            columns: list[str],
            gcs_bucket: str,
            use_direct_write: bool
    ):
        Destination.__init__(self, overwrite)
        BigQuery.__init__(self, project, dataset)
        self.db_table_destination = db_table_destination
        self.client_bigquery = bigquery.Client()
        self.columns = columns
        self.gcs_bucket = gcs_bucket
        self.use_direct_write = use_direct_write

    def write(self, df: DataFrame):
        if self.overwrite:
            mode_write = "overwrite"
        else:
            mode_write = "append"

        tmp_df_writer: DataFrameWriter = (
            df.write.format(self.format)
            .option("table", f"{self.project}.{self.dataset}.{self.db_table_destination}")
            .mode(mode_write)
        )

        if not self.use_direct_write:
            tmp_df_writer.option("temporaryGcsBucket", self.gcs_bucket)
        else:
            tmp_df_writer.option("writeMethod", "direct")

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

        logger.debug(f"final_query: {final_query}")

        job = self.client_bigquery.query(final_query)
        job.result()
        return job.num_dml_affected_rows

    #Request is prohibited by organization's policy. Fare insert in questo modo in bigquery non è possibile per restrizioni di organizzazione
    def write_rows(self, rows):
        table_id = self.resolve_destination()

        if self.overwrite:
            self.client_bigquery.query(f"TRUNCATE TABLE {table_id}").result()

        batch = []
        for row in rows:
            batch.append(dict(zip(self.columns, row)))

        table = self.client_bigquery.get_table(table_id)
        self.client_bigquery.insert_rows(table, batch)

    def resolve_destination(self) -> str:
        return f"{self.project}.{self.dataset}.{self.db_table_destination}"

