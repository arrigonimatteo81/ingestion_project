from common.dataproc import DataprocService
from metadata.loader.metadata_loader import OrchestratorMetadata
from metadata.models.tab_groups import Group
from metadata.models.tab_tasks import Task, TaskType
import unittest

TEST_RUN_ID = "20250115_1013"
TEST_APPLICATION_CONF = "application.conf"


class MockOrchestratorRepository(OrchestratorMetadata):
    def __init__(self) -> None:
        pass

    def get_all_tasks_in_group(self, groups: [str]) -> [Group]:
        return [Group("DM", "GRP1"), Group("KPI1", "GRP1"),
                Group("KPI2", "GRP1"), Group("REPORT1", "GRP1"),
                Group("REPORT2", "GRP1")]

    def get_task(self, task_id):
        return Task("id_task_1", "source_id_1", "destination_id_1", "Task di test", "profilo_di_test", True)

    def get_task_configuration(self, config_task):
        return TaskType("profilo_di_test", "descrizione profilo di test", "main_file_python.py",
                        ["additional_file_1.py", "additional_file_2.py"], ["jar_file_uri"], ["additional_jar_file_uri"])


class TestDataprocService(unittest.TestCase):
    def setUp(self):
        self.orchestrator_repo = MockOrchestratorRepository()

    def test_instantiate_task(self):
        task1_job = DataprocService.instantiate_task(
            task_id="id_task_1",
            repository=self.orchestrator_repo,
            run_id=TEST_RUN_ID,
            config_file=TEST_APPLICATION_CONF,
        )
        """self.assertIn("jobs", template_request)  # asserts jobs are present
        self.assertEqual(
            len(template_request["jobs"]), len(self.tasks_dag)
        )  # asserts exactly the correct number of jobs are present
        task1_job = template_request["jobs"][0]"""
        self.assertEqual(
            task1_job.get("step_id"), "step-id_task_1"
        )  # assert correct task id
        self.assertEqual(
            task1_job.get("pyspark_job").get("main_python_file_uri"),
            "main_file_python.py",
        )  # assert main python file
        self.assertEqual(
            task1_job.get("pyspark_job").get("python_file_uris"), ["additional_file_1.py", "additional_file_2.py"]
        )  # assert python files
        self.assertEqual(
            task1_job.get("pyspark_job").get("jar_file_uris"), ["jar_file_uri"]
        )  # assert files
        self.assertEqual(
            task1_job.get("pyspark_job").get("file_uris"), ["additional_jar_file_uri"]
        )

    def test_create_to_do_list_of_one_item(self):
        todo_list = DataprocService.create_todo_list(TEST_APPLICATION_CONF, self.orchestrator_repo, TEST_RUN_ID,
                                                     {"id_task_1"})
        self.assertEqual(1, len(todo_list))

    def test_create_to_do_list_of_two_items(self):
        todo_list = DataprocService.create_todo_list(TEST_APPLICATION_CONF, self.orchestrator_repo, TEST_RUN_ID,
                                                     {"id_task_1", "id_task_2"})
        self.assertEqual(2, len(todo_list))
