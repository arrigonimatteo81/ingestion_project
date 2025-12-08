from connectors.source_strategies import JDBCSource, CSVSource, DriverSource, SourceStrategy


class SourceFactory:

    @staticmethod
    def create(spark, src_cfg, conn_cfg) -> SourceStrategy:
        t = conn_cfg["conn_type"]

        if t == "jdbc":
            return JDBCSource(spark, conn_cfg, src_cfg["src_table"], src_cfg["src_query"])

        if t == "csv":
            return CSVSource(spark, src_cfg["file_path"])

        if t == "driver":
            return DriverSource(spark, conn_cfg, src_cfg["src_table"], src_cfg["src_query"])

        raise ValueError(f"Sorgente non supportata: {t}")
