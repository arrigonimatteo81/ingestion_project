import importlib

class SourceStrategy:
    def read(self):
        raise NotImplementedError


class JDBCSource(SourceStrategy):
    def __init__(self, spark, cfg, table=None, query=None):
        self.spark = spark
        self.cfg = cfg
        self.table = table
        self.query = query

    def read(self):
        df_reader = (
            self.spark.read.format("jdbc")
            .option("url", self.cfg["jdbc_url"])
            .option("user", self.cfg["user"])
            .option("password", self.cfg["password"])
            .option("driver", self.cfg["extra_params"].get("driver"))
        )
        if self.table:
            df_reader = df_reader.option("dbtable", self.table)
        else:
            df_reader = df_reader.option("query", self.query)
        return df_reader.load()


class CSVSource(SourceStrategy):
    def __init__(self, spark, file_path):
        self.spark = spark
        self.file_path = file_path

    def read(self):
        return (self.spark.read
                .option("header", True)
                .option("inferSchema", True)
                .csv(self.file_path))


class DriverSource(SourceStrategy):
    def __init__(self, spark, cfg, table, query):
        self.spark = spark
        self.cfg = cfg
        self.table = table
        self.query = query

    def read(self):
        module = importlib.import_module(self.cfg["extra_params"]["module"])
        conn = module.connect(
            host=self.cfg["host"],
            port=self.cfg["port"],
            user=self.cfg["user"],
            password=self.cfg["password"],
            dbname=self.cfg["db_name"]
        )
        import pandas as pd
        pdf = pd.read_sql(self.query or f"SELECT * FROM {self.table}", conn)
        conn.close()
        return self.spark.createDataFrame(pdf)
