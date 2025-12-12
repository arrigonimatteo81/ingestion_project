import os
import unittest

from orchestrator.manager import OrchestratorManager
from tests.orchestrator.test_metadata import TestOrchestratorMetadata

test_dir = os.path.dirname(os.path.abspath(__file__))
TEST_CONFIG_FILE = f"{test_dir}/resources/application.conf"

TEST_REPOSITORY: TestOrchestratorMetadata = TestOrchestratorMetadata()


class TestOrchestrator(unittest.TestCase):
    def setUp(self):
        self.orchestrator = OrchestratorManager(
            run_id="TestOrchestrator",
            config_file=TEST_CONFIG_FILE,
            repository=TEST_REPOSITORY,
        )

    def test_fetch_unique_tasks_in_group(self):
        groups = ["GRP_REPORT1", "GRP_REPORT2"]
        actual_tasks: set = self.orchestrator._fetch_tasks_ids_in_groups(groups)
        expected_tasks: set = {"DM", "KPI1", "REPORT1", "KPI2", "REPORT2"}
        self.assertEqual(actual_tasks, expected_tasks)