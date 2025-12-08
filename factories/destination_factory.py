from connectors.destination_writers import (
    JDBCDestination,
    BigQueryDestination,
    FileDestination
)

class DestinationFactory:

    @staticmethod
    def create(dest_cfg):
        t = dest_cfg["dest_type"]

        if t == "jdbc":
            return JDBCDestination(dest_cfg, dest_cfg["table_name"])

        if t == "bigquery":
            return BigQueryDestination(dest_cfg, dest_cfg["table_name"])

        if t == "file":
            return FileDestination(dest_cfg, dest_cfg["table_name"])

        raise ValueError(f"Destinazione non supportata: {t}")
