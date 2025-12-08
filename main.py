from pyspark.sql import SparkSession
from metadata.loader.metadata_loader import MetadataLoader
from ingestion.orchestrator import IngestionOrchestrator

spark = SparkSession.builder.appName("UniversalIngestion").getOrCreate()

meta_db_conn = {
    "host": "localhost",
    "port": 5432,
    "user": "meta_user",
    "password": "meta_pwd",
    "dbname": "metadata_db"
}

loader = MetadataLoader(meta_db_conn)

connections = loader.load_connections()
sources = loader.load_sources()
destinations = loader.load_destinations()

orchestrator = IngestionOrchestrator(spark, connections, destinations)

for src_cfg in sources:
    orchestrator.run_pipeline(src_cfg)
