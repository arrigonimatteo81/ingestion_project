from pyspark.sql import DataFrame, DataFrameWriter

from db.database_aware import DatabaseAware
from metadata.models.tab_jdbc import  JDBC
from processor.destinations.base import Destination, NativeWritable, SparkWritable


class TableJDBCDestination(SparkWritable,NativeWritable, JDBC, DatabaseAware, Destination):

    def write_rows(self, rows):

        placeholders = ", ".join(["%s"] * len(self.columns))
        cols = ", ".join(self.columns)

        sql = f"""INSERT INTO {self.db_table_destination} ({cols})
                  VALUES ({placeholders})"""

        with self.create_database() as db:
            db.connect()
            if self.overwrite:
                db.execute(f"TRUNCATE TABLE {self.db_table_destination}")
            db.execute_many(sql, rows)
            db.close()
            return


    def __init__(
            self,
            username: str,
            password: str,
            driver: str,
            url: str,
            db_table_destination: str,
            overwrite: bool,
            columns: list[str]
    ):
        Destination.__init__(self, overwrite)
        JDBC.__init__(self, username, password, driver, url)
        self.db_table_destination = db_table_destination
        self.columns = columns

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