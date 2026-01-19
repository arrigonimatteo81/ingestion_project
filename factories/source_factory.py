from typing import Optional

from common.secrets import SecretRetriever
from common.utils import extract_field_from_file, get_logger
from metadata.loader.metadata_loader import ProcessorMetadata, MetadataLoader
from metadata.models.tab_bigquery import TabBigQuerySource
from metadata.models.tab_config_partitioning import TabConfigPartitioning
from metadata.models.tab_file import TabFileSource
from metadata.models.tab_jdbc import TabJDBCSource
from processor.domain import SourceType, FileFormat
from processor.sources.bigquery_source import TableBigQuerySource, QueryBigQuerySource
from processor.sources.file_sources import ExcelFileSource, CsvFileSource, ParquetFileSource
from processor.sources.jdbc_sources import TableJDBCSource, QueryJDBCSource

logger = get_logger(__name__)



class SourceFactory:

    @staticmethod
    def create_source(source_type: str, source_id: str, config_file: str, cp : TabConfigPartitioning = None,
                      opt_secret_retriever: Optional[SecretRetriever] = None):

        connection_string = extract_field_from_file(config_file, "CONNECTION_PARAMS")
        repository = ProcessorMetadata(MetadataLoader(connection_string))
        try:
            if source_type.upper() == SourceType.JDBC.value:
                jdbc_source :TabJDBCSource = repository.get_jdbc_source_info(source_id)
                #semplice gestione del secret retriever, serve solo per jdbc perchè per ora è l'unica configurazione che richiede password
                if jdbc_source.pwd.upper().startswith("SECRET::"):
                    secret_pwd = opt_secret_retriever.retrieve_secret(f"secrets.{jdbc_source.username}")
                else:
                    secret_pwd = jdbc_source.pwd
                if jdbc_source.tablename:
                    return TableJDBCSource(jdbc_source.username,secret_pwd, jdbc_source.driver, jdbc_source.url,
                                           jdbc_source.tablename)
                elif jdbc_source.query_text:
                    return QueryJDBCSource(jdbc_source.username, secret_pwd, jdbc_source.driver,
                                           jdbc_source.url, jdbc_source.query_text,
                                           cp.partitioning_expression,cp.num_partitions)
            elif source_type.upper() == SourceType.FILE.value:
                file_source: TabFileSource = repository.get_file_source_info(source_id)
                if file_source.file_type.upper() == FileFormat.EXCEL.value:
                    return ExcelFileSource(file_source.path, file_source.sheet)
                elif file_source.file_type.upper() == FileFormat.CSV.value:
                    return CsvFileSource(file_source.path, file_source.csv_separator)
                elif file_source.file_type.upper() == FileFormat.PARQUET.value:
                    return ParquetFileSource(file_source.path)
            elif source_type.upper() == SourceType.BIGQUERY.value:
                bigquery_source: TabBigQuerySource = repository.get_bq_source_info(source_id)
                if bigquery_source.tablename:
                    return TableBigQuerySource(bigquery_source.project, bigquery_source.dataset, bigquery_source.tablename)
                elif bigquery_source.query_text:
                    return QueryBigQuerySource(bigquery_source.project, bigquery_source.dataset, bigquery_source.query_text)
            else:
                logger.error("Unsupported source type!!!")
                raise ValueError(f"Unsupported source type: {source_type}")
        except Exception as exc:
            logger.error(f"Failed to create source: {exc}")
            raise