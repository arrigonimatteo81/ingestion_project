from dataclasses import dataclass
from google.cloud.dataproc_v1 import LoggingConfig

class TaskType:

    def __init__(
            self,
            name: str,
            description: str,
            main_python_file_uri: str,
            additional_python_file_uris: list[
                str
            ] = None,
            # python_file_uris according to https://cloud.google.com/python/docs/reference/dataproc/latest/google.cloud.dataproc_v1.types.PySparkJob
            jar_file_uris: list[str] = None,
            additional_file_uris: list[
                str
            ] = None,
            # file_uris according to https://cloud.google.com/python/docs/reference/dataproc/latest/google.cloud.dataproc_v1.types.PySparkJob
            archive_file_uris: list[
                str
            ] = None,
            # archive_uris according to https://cloud.google.com/python/docs/reference/dataproc/latest/google.cloud.dataproc_v1.types.PySparkJob
            logging_config: dict = None,
            dataproc_properties: dict = None,
            async_submit: bool = False
    ):
        self._main_python_file_uri = main_python_file_uri
        self._name = name
        self._description = description
        self._additional_python_file_uris = additional_python_file_uris or []
        self._jar_file_uris = jar_file_uris or []
        self._additional_file_uris = additional_file_uris or []
        self._archive_file_uris = archive_file_uris or []
        self._dataproc_properties = dataproc_properties or {}
        self._async_submit = async_submit

        if not self._main_python_file_uri:
            raise ValueError("main_python_file_uri must not be empty")

        self._logging_config = self.map_loggin_config_to_gcloud_skd(logging_config)

    def to_dict(self):
        level_mapping = {
            LoggingConfig.Level.DEBUG: "DEBUG",
            LoggingConfig.Level.INFO: "INFO",
            LoggingConfig.Level.WARN: "WARN",
            LoggingConfig.Level.ERROR: "ERROR",
            LoggingConfig.Level.FATAL: "FATAL",
        }
        logging_config_dict = {}
        if self._logging_config:
            logging_config_dict = {
                key: level_mapping.get(level, str(level))
                for key, level in self._logging_config.driver_log_levels.items()
            }

        return {
            "main_python_file_uri": self._main_python_file_uri,
            "name": self._name,
            "description": self._description,
            "additional_python_file_uris": self._additional_python_file_uris,
            "jar_file_uris": self._jar_file_uris,
            "additional_file_uris": self._additional_file_uris,
            "archive_file_uris": self._archive_file_uris,
            "dataproc_properties": self._dataproc_properties,
            "async_submit": self._async_submit,
            "logging_config": logging_config_dict
        }

    @property
    def main_python_file_uri(self) -> str:
        return self._main_python_file_uri

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str:
        return self._description

    @property
    def additional_python_file_uris(self) -> list[str]:
        return self._additional_python_file_uris

    @property
    def jar_file_uris(self) -> list[str]:
        return self._jar_file_uris

    @property
    def additional_file_uris(self) -> list[str]:
        return self._additional_file_uris

    @property
    def archive_file_uris(self) -> list[str]:
        return self._archive_file_uris

    @property
    def logging_config(self) -> LoggingConfig:
        return self._logging_config

    @property
    def dataproc_properties(self) -> dict:
        return self._dataproc_properties

    @property
    def async_submit(self) -> bool:
        return self._async_submit

    def map_loggin_config_to_gcloud_skd(self, config_dict: dict) -> LoggingConfig:
        if not config_dict:
            # handle empty logging config returning a basic
            return LoggingConfig(
                driver_log_levels={
                    "root": LoggingConfig.Level.INFO,
                    "com.google": LoggingConfig.Level.WARN,
                    "org.apache": LoggingConfig.Level.WARN,
                }
            )
        driver_log_levels = {
            key: self._create_level(level) for key, level in config_dict.items()
        }
        return LoggingConfig(driver_log_levels=driver_log_levels)

    def _create_level(self, level: str) -> LoggingConfig.Level:
        MAPPING = {
            "DEBUG": LoggingConfig.Level.DEBUG,
            "INFO": LoggingConfig.Level.INFO,
            "WARN": LoggingConfig.Level.WARN,
            "ERROR": LoggingConfig.Level.ERROR,
            "FATAL": LoggingConfig.Level.FATAL,
        }
        if level not in MAPPING:
            raise ValueError(f"Unknown logging level: {level}")
        return MAPPING[level]

    def __repr__(self):
        return (
            f"TaskType("
            f"main_python_file_uri={self._main_python_file_uri!r}, "
            f"name={self._name!r}, "
            f"description={self._description!r}, "
            f"additional_python_file_uris={self._additional_python_file_uris!r}, "
            f"jar_file_uris={self._jar_file_uris!r}, "
            f"additional_file_uris={self._additional_file_uris!r}, "
            f"archive_file_uris={self._archive_file_uris!r}, "
            f"logging_config={self._logging_config!r}, "
            f"dataproc_properties={self._dataproc_properties!r}, "
            f"async_submit={self._async_submit!r}"
            f")"
        )

@dataclass
class Task:
    id: str
    source_id: str
    destination_id: str
    description: str
    config_profile: str
    is_blocking: bool = True