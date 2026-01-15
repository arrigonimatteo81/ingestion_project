from typing import Optional

from common.environment import Environment

class Configuration:
    """
    A class that holds arbitrary configuration key-value pairs. Keys and values are strings.
    """

    def __init__(self, configurations: dict = None) -> None:
        self._configurations: dict = configurations if configurations else {}

    def get(self, key: str) -> Optional[str]:
        return self._configurations.get(key, None)

    @property
    def configurations(self):
        return self._configurations

class DataprocConfiguration:
    """
    A class that holds Dataproc-related configurations
    """
    def __init__(
            self,
            project: str,
            region: str,
            cluster_name: str,
            environment: Environment,
            poll_sleep_time_seconds: int = 5
    ):
        if project:
            self._project=project
        else: raise ValueError("The 'project' parameter must be provided and cannot be empty.")

        if region:
            self._region=region
        else:
            raise ValueError(
                "The 'region' parameter must be provided and cannot be empty."
            )
        if cluster_name:
            self._cluster_name = cluster_name
        else:
            raise ValueError(
                "The 'cluster_name' parameter must be provided and cannot be empty."
            )
        if environment:
            self._environment = environment
        else:
            raise ValueError(
                "The 'environment' parameter must be provided and cannot be empty."
            )

        self._poll_sleep_time_seconds = poll_sleep_time_seconds


    def __repr__(self):
        return f"DataprocConfiguration(project={self.project},region={self.region},cluster_name={self.cluster_name},poll_sleep_time_seconds={self.poll_sleep_time_seconds},environment={str(self.environment)})"

    @classmethod
    def from_configuration(cls, configuration: Configuration):
        project = configuration.get("project")
        region = configuration.get("region")
        cluster_name = configuration.get("cluster_name")
        environment: Environment = Environment.from_string(configuration.get("environment"))
        poll_sleep_time_seconds = None
        if configuration.get("poll_sleep_time_seconds"):
            poll_sleep_time_seconds = int(configuration.get("poll_sleep_time_seconds"))

        return cls(
            project=project,
            region=region,
            cluster_name=cluster_name,
            poll_sleep_time_seconds=poll_sleep_time_seconds,
            environment=environment
        )

    @property
    def project(self) -> str:
        return self._project

    @property
    def region(self) -> str:
        return self._region

    @property
    def cluster_name(self) -> str:
        return self._cluster_name

    @property
    def poll_sleep_time_seconds(self) -> int:
        return self._poll_sleep_time_seconds

    @property
    def environment(self) -> Environment:
        return Environment.from_string(self._environment.value)
