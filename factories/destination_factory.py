from common.utils import extract_field_from_file, get_logger
from metadata.loader.metadata_loader import ProcessorMetadata
from metadata.models.tab_file import TabFileDest
from metadata.models.tab_jdbc import TabJDBCDest
from processor.destinations.base import Destination
from processor.destinations.file_destinations import CsvFileDestination, ParquetFileDestination
from processor.destinations.jdbc_destinations import TableJDBCDestination
from processor.domain import DestinationType, FileFormat

logger = get_logger(__name__)

class DestinationFactory:
    @staticmethod
    def create_destination(destination_type: str, destination_id: str, config_file: str) -> Destination:
        connection_string = extract_field_from_file(config_file, "CONNECTION_PARAMS")
        repository = ProcessorMetadata(connection_string)
        try:
            if destination_type.upper() == DestinationType.JDBC.value:
                jdbc_destination :TabJDBCDest = repository.get_jdbc_dest_info(destination_id)
                return TableJDBCDestination(jdbc_destination.username,jdbc_destination.pwd, jdbc_destination.driver,
                                            jdbc_destination.url, jdbc_destination.tablename, jdbc_destination.overwrite)
            elif destination_type.upper() == DestinationType.FILE.value:
                file_destination: TabFileDest = repository.get_file_dest_info(destination_id)
                if file_destination.file_type.upper() == FileFormat.CSV.value:
                    return CsvFileDestination(file_destination.path, file_destination.csv_separator,file_destination.overwrite)
                elif file_destination.file_type.upper() == FileFormat.PARQUET.value:
                    return ParquetFileDestination(file_destination.path, file_destination.overwrite)
            #if source_type.upper() == SourceType.BIGQUERY.value:
            else:
                logger.error("Unsupported destination type!!!")
                raise ValueError(f"Unsupported destination type: {destination_type}")
        except Exception as exc:
            logger.error(f"Failed to create source: {exc}")
            raise