from connectors.source_strategies import SourceStrategy
from factories.source_factory import SourceFactory
from factories.destination_factory import DestinationFactory


class IngestionOrchestrator:

    def __init__(self, spark, connections, destinations):
        self.spark = spark
        self.connections = connections
        self.destinations = destinations

    def run_pipeline(self, src_cfg):
        conn_cfg = self.connections[src_cfg["conn_id"]]
        dest_cfg = self.destinations[src_cfg["id_dest"]]

        source: SourceStrategy = SourceFactory.create(self.spark, src_cfg, conn_cfg)
        df = source.read()

        dest = DestinationFactory.create(dest_cfg)
        dest.write(df)

        print(f"Pipeline eseguita: sorgente {src_cfg['id_src']} -> destinazione {src_cfg['id_dest']}")
