import logging

from common.const import NAME_OF_PARTITIONING_COLUMN
from common.utils import extract_field_from_file, get_logger
from factories.database_factory import DatabaseFactory
from factories.partition_configuration_factory import PartitioningConfigurationFactory
from metadata.loader.metadata_loader import ProcessorMetadata
from metadata.models.tab_file import TabFileSource
from metadata.models.tab_jdbc import TabJDBCSource
from processor.domain import SourceType, FileFormat
from processor.sources.base import Source
from processor.sources.file_sources import ExcelFileSource, CsvFileSource, ParquetFileSource
from processor.sources.jdbc_sources import TableJDBCSource, QueryJDBCSource
from processor.sources.partitioning import PartitioningConfiguration

logger = get_logger(__name__)



class SourceFactory:

    @staticmethod
    def create_source(source_type: str, source_id: str, config_file: str):

        connection_string = extract_field_from_file(config_file, "CONNECTION_PARAMS")
        repository = ProcessorMetadata(connection_string)
        try:
            if source_type.upper() == SourceType.JDBC.value:
                jdbc_source :TabJDBCSource = repository.get_jdbc_source_info(source_id)
                if jdbc_source.tablename:
                    return TableJDBCSource(jdbc_source.username,jdbc_source.pwd, jdbc_source.driver, jdbc_source.url, jdbc_source.tablename)
                elif jdbc_source.query_text:
                    if jdbc_source.partitioning_expression and jdbc_source.num_partitions:

                        db_factory: DatabaseFactory = DatabaseFactory({"user": jdbc_source.username, "password": jdbc_source.pwd, "url": jdbc_source.url})
                        pc_factory: PartitioningConfigurationFactory = PartitioningConfigurationFactory(db_factory)
                        partitioning_cfg: PartitioningConfiguration = pc_factory.create_partitioning_configuration(jdbc_source.partitioning_expression,
                                                                                                                   jdbc_source.num_partitions,
                                                                                                                   jdbc_source.query_text)
                        return QueryJDBCSource(jdbc_source.username, jdbc_source.pwd, jdbc_source.driver,
                                               jdbc_source.url,(f"(select *, {jdbc_source.partitioning_expression} as {NAME_OF_PARTITIONING_COLUMN} "
                                                 f"from ({jdbc_source.query_text}) tab ) as subquery"), partitioning_cfg)
                    else:
                        query_transformed_per_spark: str = f"({jdbc_source.query_text}) as subquery"
                        return QueryJDBCSource(jdbc_source.username, jdbc_source.pwd, jdbc_source.driver, jdbc_source.url, query_transformed_per_spark)
            elif source_type.upper() == SourceType.FILE.value:
                file_source: TabFileSource = repository.get_file_source_info(source_id)
                if file_source.file_type.upper() == FileFormat.EXCEL.value:
                    return ExcelFileSource(file_source.path, file_source.sheet)
                elif file_source.file_type.upper() == FileFormat.CSV.value:
                    return CsvFileSource(file_source.path, file_source.csv_separator)
                elif file_source.file_type.upper() == FileFormat.PARQUET.value:
                    return ParquetFileSource(file_source.path)
            #if source_type.upper() == SourceType.BIGQUERY.value:
            else:
                logger.error("Unsupported source type!!!")
                raise ValueError(f"Unsupported source type: {source_type}")
        except Exception as exc:
            logger.error(f"Failed to create source: {exc}")
            raise