class DestinationWriter:
    def write(self, df):
        raise NotImplementedError


class JDBCDestination(DestinationWriter):
    def __init__(self, cfg, table):
        self.cfg = cfg
        self.table = table

    def write(self, df):
        (df.write.format("jdbc")
            .option("url", self.cfg["jdbc_url"])
            .option("dbtable", self.table)
            .option("user", self.cfg["user"])
            .option("password", self.cfg["password"])
            .option("driver", self.cfg["extra_params"]["driver"])
            .mode("overwrite")
            .save())


class BigQueryDestination(DestinationWriter):
    def __init__(self, cfg, table):
        self.cfg = cfg
        self.table = table

    def write(self, df):
        (df.write.format("bigquery")
            .option("table", f"{self.cfg['gcp_project']}.{self.cfg['gcp_dataset']}.{self.table}")
            .mode(self.cfg["extra_params"].get("write_mode", "append"))
            .save())


class FileDestination(DestinationWriter):
    def __init__(self, cfg, file_name):
        self.path = f"{cfg['bucket_path']}/{file_name}"
        self.format = cfg["extra_params"].get("format", "parquet")

    def write(self, df):
        df.write.mode("overwrite").format(self.format).save(self.path)
