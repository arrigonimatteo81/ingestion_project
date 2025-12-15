from pyspark.sql import DataFrame, DataFrameWriter

from metadata.models.tab_jdbc import  JDBC
from processor.destinations.base import Destination


class TableJDBCDestination(Destination,JDBC):

    def __init__(
            self,
            username: str,
            password: str,
            driver: str,
            url: str,
            db_table_destination: str,
            overwrite: bool,
    ):
        Destination.__init__(self, overwrite)
        JDBC.__init__(self, username, password, driver, url)
        self.db_table_destination = db_table_destination

    def write(self, df: DataFrame):
        if self.overwrite:
            mode_write = "overwrite"
        else:
            mode_write = "append"

        tmp_df_writer: DataFrameWriter = (
            df.write.format(self.format)
            .option("url", self.url)
            .option("dbtable", self.db_table_destination)
            .option("user", self.username)
            .option("password", self.password)
            .option("driver", self.driver)
            .mode(mode_write)
        )

        tmp_df_writer.save()