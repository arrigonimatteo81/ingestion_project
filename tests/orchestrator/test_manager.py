import os
import unittest
from unittest.mock import MagicMock

from common.result import OperationResult
from metadata.models.tab_tasks import TaskSemaforo
from orchestrator.manager import IngestionOrchestratorManager

test_dir = os.path.dirname(os.path.abspath(__file__))
CONF_PATH = f"{test_dir}/resources/application.conf"
class TestOrchestratorManager(unittest.TestCase):

    def setUp(self):
        self.mock_repo = MagicMock()
        self.mock_repo.get_all_tasks_in_group.return_value = [
            TaskSemaforo("uid", "source_id", "destination_id", "tipo_caricamento", {"key":"val"}, {"key":"val"}, True),
        ]
        self.mock_repo.get_all_configurations.return_value = {"region":"europe-west12","poll_sleep_time_seconds":60,"cluster_name":	"dprcpclt-isp-nplg0-svil-ew12-01",
                                                            "environment": "svil", "project": "prj-isp-nplg0-appl-svil-001",
                                                              "bucket": "bkt-isp-nplg0-dptmp-svil-001-ew12",
                                                              "ingestion_max_contemporary_tasks":	8}

        self.manager = IngestionOrchestratorManager(
            run_id="r1",
            config_file=CONF_PATH,
            groups=["g1"],
            repository=self.mock_repo
        )


    def test_start_with_tasks(self):
        result = self.manager.start()
        self.assertIsInstance(result, OperationResult)
        self.assertTrue(result.successful)


    def test_start_no_tasks(self):
        self.mock_repo.get_all_tasks_in_group.return_value = []
        result = self.manager.start()
        self.assertFalse(result.successful)
        self.assertIn("No tasks found", result.description)


    if __name__ == "__main__":
        unittest.main()