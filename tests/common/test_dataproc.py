import unittest
from unittest.mock import Mock

from common.configuration import DataprocConfiguration
from common.dataproc import DataprocService
from common.environment import Environment
from metadata.loader.metadata_loader import OrchestratorMetadata
from metadata.models.tab_tasks import TaskType, TaskSemaforo

TEST_RUN_ID = "20250115_1013"
TEST_APPLICATION_CONF = "application.conf"

TEST_POLL_SLEEP_TIME_SECONDS = 120
TEST_CLUSTER = "test-cluster"
TEST_CLUSTER_LABELS_JSON = '{"acronimo": "fdir0", "label2": "val2"}'
TEST_CLUSTER_LABELS: dict = {"acronimo": "fdir0", "label2": "val2"}
TEST_PROJECT_NAME = "test-project"
TEST_REGION = "europe-west12"

TEST_DATAPROC_CONFIGURATION = DataprocConfiguration(
    project=TEST_PROJECT_NAME,
    region=TEST_REGION,
    cluster_name=TEST_CLUSTER,
    poll_sleep_time_seconds=TEST_POLL_SLEEP_TIME_SECONDS,
    environment=Environment.SVIL,
)


class MockOrchestratorRepository(OrchestratorMetadata):
    def __init__(self) -> None:
        pass

    """def get_all_tasks_in_group(self,groups: [str]) -> [TaskSemaforo]:
        return [TaskSemaforo("uid1","source_1","destination_1","gruppo_1",{"k1_1":"key1_1","k2":"key2_1"}, {"p1_1":"param1_1", "p2_1": "param2_1"}),
                TaskSemaforo("uid2","source_2","destination_2","gruppo_1",{"k1_2":"key1_2","k2":"key2_2"}, {"p1_2":"param1_2", "p2_2": "param2_2"}),]

    def get_task(self, task_id):
        return TaskSemaforo("uid","source_id", "destination_id", "gruppo", {"cod_provenienza":"PR", "cod_abi": "3239", "cod_tabella":"tabella"},
                            {"cod_colonna_valore" :"colonna_valore", "max_data_va": "20251230090600"})
"""
    def get_task_configuration(self, config_task):
        return TaskType("source_id-03239-PR", "descrizione profilo di test", "main_file_python.py",
                        ["additional_file_1.py", "additional_file_2.py"], ["jar_file_uri"], ["additional_jar_file_uri"])



class ToTestDataprocService(unittest.TestCase):
    def setUp(self):
        self.orchestrator_repo = MockOrchestratorRepository()


    def test_instantiate_task(self):
        task1_job = DataprocService.instantiate_task(
            task=TaskSemaforo('uid1','source_1','destination_1','gruppo_1',{'k1_1':'key1_1','k2':'key2_1'}, {'p1_1':'param1_1', 'p2_1': 'param2_1'}),
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
            task1_job.get("step_id"), "step-uid1"
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

#test ripetuto anche nella test_payload,. qui dal punto di vista di dataproc, di la dal punto di vista puramente di json
    def test_task_payload(self):
        task1_job = DataprocService.instantiate_task(
            task=TaskSemaforo('uid1','source_1','destination_1','gruppo_1',{'k1_1':'key1_1','k2':'key2_1'}, {'p1_1':'param1_1', 'p2_1': 'param2_1'}),
            repository=self.orchestrator_repo,
            run_id=TEST_RUN_ID,
            config_file=TEST_APPLICATION_CONF,
        )
        self.assertEqual(task1_job.get("pyspark_job").get("args")[3],
                         '{"uid": "uid1", "source_id": "source_1", "destination_id": "destination_1", "tipo_caricamento": "gruppo_1", "key": {"k1_1": "key1_1", "k2": "key2_1"}, "query_params": {"p1_1": "param1_1", "p2_1": "param2_1"}}')

    def test_number_of_workflows_create_to_do_list_of_one_workflow_of_one_task(self):
        todo_list = DataprocService.create_todo_list(TEST_APPLICATION_CONF, self.orchestrator_repo, TEST_RUN_ID,
                                                     [TaskSemaforo("uid1", "source_1", "destination_1", "gruppo_1",
                                                                   {"k1_1": "key1_1", "k2": "key2_1"},
                                                                   {"p1_1": "param1_1", "p2_1": "param2_1"})],
                                                     5)
        self.assertEqual(1, len(todo_list))

    def test_number_of_tasks_create_to_do_list_of_one_workflow_of_two_tasks(self):
        todo_list = DataprocService.create_todo_list(TEST_APPLICATION_CONF, self.orchestrator_repo, TEST_RUN_ID,
                                                     [TaskSemaforo("uid1", "source_1", "destination_1", "gruppo_1",
                                                                   {"k1_1": "key1_1", "k2": "key2_1"},
                                                                   {"p1_1": "param1_1", "p2_1": "param2_1"}),
                                                      TaskSemaforo("uid2", "source_2", "destination_2", "gruppo_1",
                                                                   {"k1_2": "key1_2", "k2": "key2_2"},
                                                                   {"p1_2": "param1_2", "p2_2": "param2_2"})],
                                                    5)

        self.assertEqual(2, len(todo_list))

    def test_number_of_tasks_create_to_do_list_with_one_dependence(self):
        todo_list = DataprocService.create_todo_list(TEST_APPLICATION_CONF, self.orchestrator_repo, TEST_RUN_ID,
                                                    [TaskSemaforo("uid1", "source_1", "destination_1", "gruppo_1",
                                                                  {"k1_1": "key1_1", "k2": "key2_1"},
                                                                  {"p1_1": "param1_1", "p2_1": "param2_1"}),
                                                     TaskSemaforo("uid2", "source_2", "destination_2", "gruppo_1",
                                                                  {"k1_2": "key1_2", "k2": "key2_2"},
                                                                  {"p1_2": "param1_2", "p2_2": "param2_2"}),
                                                     TaskSemaforo("uid3", "source_3", "destination_4", "gruppo_1",
                                                                  {"k1_3": "key1_3", "k2": "key2_3"},
                                                                  {"p1_3": "param1_3", "p2_1": "param2_3"}),
                                                     TaskSemaforo("uid4", "source_4", "destination_4", "gruppo_1",
                                                                  {"k1_4": "key1_4", "k2": "key2_4"},
                                                                  {"p1_4": "param1_4", "p2_4": "param2_4"})
                                                     ],
                                                    3)

        self.assertEqual('step-uid1', ",".join(todo_list[1].get('prerequisite_step_ids')))


    def test_number_of_tasks_create_to_do_list_with_one_heavy(self):
        todo_list = DataprocService.create_todo_list(TEST_APPLICATION_CONF, self.orchestrator_repo, TEST_RUN_ID,
                                                    [TaskSemaforo("uid1", "source_1", "destination_1", "gruppo_1",
                                                                  {"k1_1": "key1_1", "k2": "key2_1"},
                                                                  {"p1_1": "param1_1", "p2_1": "param2_1"},is_heavy=True),
                                                     TaskSemaforo("uid2", "source_2", "destination_2", "gruppo_1",
                                                                  {"k1_2": "key1_2", "k2": "key2_2"},
                                                                  {"p1_2": "param1_2", "p2_2": "param2_2"}),
                                                     TaskSemaforo("uid3", "source_3", "destination_4", "gruppo_1",
                                                                  {"k1_3": "key1_3", "k2": "key2_3"},
                                                                  {"p1_3": "param1_3", "p2_1": "param2_3"}),
                                                     TaskSemaforo("uid4", "source_4", "destination_4", "gruppo_1",
                                                                  {"k1_4": "key1_4", "k2": "key2_4"},
                                                                  {"p1_4": "param1_4", "p2_4": "param2_4"})
                                                     ],
                                                    3)
        self.assertEqual('step-uid2', ",".join(todo_list[2].get('prerequisite_step_ids')))

    def test_number_of_tasks_create_to_do_list_of_two_workflows_with_two_heavy(self):
        todo_list = DataprocService.create_todo_list(TEST_APPLICATION_CONF, self.orchestrator_repo, TEST_RUN_ID,
                                                    [TaskSemaforo("uid1", "source_1", "destination_1", "gruppo_1",
                                                                  {"k1_1": "key1_1", "k2": "key2_1"},
                                                                  {"p1_1": "param1_1", "p2_1": "param2_1"},is_heavy=True),
                                                     TaskSemaforo("uid2", "source_2", "destination_2", "gruppo_1",
                                                                  {"k1_2": "key1_2", "k2": "key2_2"},
                                                                  {"p1_2": "param1_2", "p2_2": "param2_2"}),
                                                     TaskSemaforo("uid3", "source_3", "destination_4", "gruppo_1",
                                                                  {"k1_3": "key1_3", "k2": "key2_3"},
                                                                  {"p1_3": "param1_3", "p2_1": "param2_3"}, is_heavy=True),
                                                     TaskSemaforo("uid4", "source_4", "destination_4", "gruppo_1",
                                                                  {"k1_4": "key1_4", "k2": "key2_4"},
                                                                  {"p1_4": "param1_4", "p2_4": "param2_4"})
                                                     ],
                                                    3)

        self.assertEqual('step-uid1', ",".join(todo_list[1].get('prerequisite_step_ids')))

    def test_number_of_tasks_create_to_do_list_of_two_workflows_with_two_heavy_and_three_normal(self):
        todo_list = DataprocService.create_todo_list(TEST_APPLICATION_CONF, self.orchestrator_repo, TEST_RUN_ID,
                                                    [TaskSemaforo("uid1", "source_1", "destination_1", "gruppo_1",
                                                                  {"k1_1": "key1_1", "k2": "key2_1"},
                                                                  {"p1_1": "param1_1", "p2_1": "param2_1"},is_heavy=True),
                                                     TaskSemaforo("uid2", "source_2", "destination_2", "gruppo_1",
                                                                  {"k1_2": "key1_2", "k2": "key2_2"},
                                                                  {"p1_2": "param1_2", "p2_2": "param2_2"}),
                                                     TaskSemaforo("uid3", "source_3", "destination_4", "gruppo_1",
                                                                  {"k1_3": "key1_3", "k2": "key2_3"},
                                                                  {"p1_3": "param1_3", "p2_1": "param2_3"}, is_heavy=True),
                                                     TaskSemaforo("uid4", "source_4", "destination_4", "gruppo_1",
                                                                  {"k1_4": "key1_4", "k2": "key2_4"},
                                                                  {"p1_4": "param1_4", "p2_4": "param2_4"}),
                                                     TaskSemaforo("uid5", "source_5", "destination_5", "gruppo_1",
                                                                  {"k1_5": "key1_5", "k2": "key2_5"},
                                                                  {"p1_5": "param1_5", "p2_5": "param2_5"})

                                                     ],3)

        self.assertEqual('step-uid2', ",".join(todo_list[3].get('prerequisite_step_ids')))

    def test_number_of_tasks_create_to_do_list_with_multiple_dependencies(self):
        todo_list = DataprocService.create_todo_list(TEST_APPLICATION_CONF, self.orchestrator_repo, TEST_RUN_ID,
                                                    [TaskSemaforo("uid1", "source_1", "destination_1", "gruppo_1",
                                                                  {"k1_1": "key1_1", "k2": "key2_1"},
                                                                  {"p1_1": "param1_1", "p2_1": "param2_1"}),
                                                     TaskSemaforo("uid2", "source_2", "destination_2", "gruppo_1",
                                                                  {"k1_2": "key1_2", "k2": "key2_2"},
                                                                  {"p1_2": "param1_2", "p2_2": "param2_2"}),
                                                     TaskSemaforo("uid3", "source_3", "destination_4", "gruppo_1",
                                                                  {"k1_3": "key1_3", "k2": "key2_3"},
                                                                  {"p1_3": "param1_3", "p2_1": "param2_3"}),
                                                     TaskSemaforo("uid4", "source_4", "destination_4", "gruppo_1",
                                                                  {"k1_4": "key1_4", "k2": "key2_4"},
                                                                  {"p1_4": "param1_4", "p2_4": "param2_4"}),
                                                     TaskSemaforo("uid5", "source_5", "destination_5", "gruppo_1",
                                                                  {"k1_5": "key1_5", "k2": "key2_5"},
                                                                  {"p1_5": "param1_5", "p2_4": "param2_5"})
                                                     ],
                                                    2)

        self.assertEqual('step-uid1', ",".join(todo_list[1].get('prerequisite_step_ids')))
        self.assertEqual('step-uid3', ",".join(todo_list[2].get('prerequisite_step_ids')))



    def test_build_labels_with_all_labels(self):
        task1_job = DataprocService.instantiate_task(
            task=TaskSemaforo('uid1', 'source_1', 'destination_1', 'gruppo_1', key={"cod_abi": 3239, "cod_tabella": "REAGDG", "cod_provenienza": "AN"},
                              query_params={"id": 120957, "cod_abi": 3239, "num_ambito": 0, "max_data_va": 20000101, "cod_provenienza": "AN",
                               "num_periodo_rif": 202511, "cod_colonna_valore": ""}, is_heavy=True),
            repository=self.orchestrator_repo,
            run_id=TEST_RUN_ID,
            config_file=TEST_APPLICATION_CONF,
        )
        expected={'run_id': '20250115_1013', 'table': 'reagdg', 'abi': '3239', 'prov': 'an', 'periodo': '202511'}

        self.assertEqual(task1_job.get("labels"), expected)

    def test_build_labels_without_all_labels(self):
        task1_job = DataprocService.instantiate_task(
            task=TaskSemaforo('uid1', 'source_1', 'destination_1', 'gruppo_1',
                              key={"cod_tabella": "REAGDG"},
                              query_params={"id": 120957, }, is_heavy=False),
            repository=self.orchestrator_repo,
            run_id=TEST_RUN_ID,
            config_file=TEST_APPLICATION_CONF,
        )
        expected = {'run_id': '20250115_1013', 'table': 'reagdg'}

        self.assertEqual(task1_job.get("labels"), expected)

