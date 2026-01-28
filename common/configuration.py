from enum import Enum
from typing import Optional

from common.environment import Environment
from google.cloud.dataproc_v1 import JobPlacement

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

class WorkflowPlacementStrategy(Enum):
    """
    Enum representing different strategies to place a workflow on a DataprocCluster

    Attributes:
        MANAGED_CLUSTER: managed_cluster via cluster configuration
        CLUSTER_SELECTOR: cluster selector via labels
    """

    MANAGED_CLUSTER = "managed_cluster"
    CLUSTER_SELECTOR = "cluster_selector"

    def __str__(self) -> str:
        """
        Returns the string representation of the Environment value.

        Returns:
            str: The string value of the Environment instance.
        """
        return self.value

    @classmethod
    def from_string(cls, workflow_placement_strategy_string: str) -> 'WorkflowPlacementStrategy':
        """
        Creates an WorkflowPlacementStrategy instance from a string in a case-insensitive way.

        Raises:
            ValueError: If the provided string doesn't match any Environment value.
        """
        normalized_string = workflow_placement_strategy_string.lower()

        for strategy in cls:
            if strategy.value == normalized_string:
                return strategy

        valid_values = [strategy.value for strategy in cls]
        raise ValueError(
            f"Invalid workflow placement strategy: '{workflow_placement_strategy_string}'. Valid values are: {valid_values}")
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


class OrchestratorConfiguration:
    """
    A class that holds Orchestrator-related configurations
    """
    def __init__(
            self,
            bucket: str,
            ingestion_max_contemporary_tasks: int = 10,
            silver_max_contemporary_tasks: int = 10
    ):
        if bucket:
            self._bucket=bucket
        else: raise ValueError("The 'bucket' parameter must be provided and cannot be empty.")

        if ingestion_max_contemporary_tasks:
            self._ingestion_max_contemporary_tasks=ingestion_max_contemporary_tasks
        else:
            raise ValueError(
                "The 'ingestion_max_contemporary_tasks' parameter must be provided and cannot be empty."
            )

        if silver_max_contemporary_tasks:
            self._silver_max_contemporary_tasks = silver_max_contemporary_tasks
        else:
            raise ValueError(
                "The 'silver_max_contemporary_tasks' parameter must be provided and cannot be empty."
            )


    def __repr__(self):
        return (f"OrchestratorConfiguration(bucket={self.bucket},"
                f"ingestion_max_contemporary_tasks={self.ingestion_max_contemporary_tasks},"
                f"silver_max_contemporary_tasks={self.silver_max_contemporary_tasks})")

    @classmethod
    def from_configuration(cls, configuration: Configuration):
        bucket = configuration.get("bucket")
        ingestion_max_contemporary_tasks = configuration.get("ingestion_max_contemporary_tasks")
        silver_max_contemporary_tasks = configuration.get("silver_max_contemporary_tasks")

        return cls(
            bucket=bucket,
            ingestion_max_contemporary_tasks=int(ingestion_max_contemporary_tasks),
            silver_max_contemporary_tasks=int(silver_max_contemporary_tasks)
        )

    @property
    def bucket(self) -> str:
        return self._bucket

    @property
    def ingestion_max_contemporary_tasks(self) -> int:
        return self._ingestion_max_contemporary_tasks

    @property
    def silver_max_contemporary_tasks(self) -> int:
        return self._silver_max_contemporary_tasks
