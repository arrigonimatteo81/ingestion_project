import unittest

from helpers.query_resolver import TaskContext, QueryResolver
from metadata.models.tab_tasks import TaskSemaforo


class TestQueryRenderer(unittest.TestCase):
    def test_return_correct_query_with_all_parameters(self):
        query_ctx = TaskContext(
                    task=TaskSemaforo("uid1", "source_id", "destination_id","gruppo_1",{"k1_1": "key1_1", "k2": "key2_1"},
                                      {"p1_1": "param1_1", "p2_1": "param2_1"}),
                    key={"k1_1": "key1_1", "k2": "key2_1"},
                    query_params={"num_periodo_rif": 202510, "colonna_valore": "valore_p1", "cod_abi": 3239, "cod_provenienza": "PR"}, registro_repo=None
                )
        query_text = QueryResolver.resolve("SELECT ${num_periodo_rif} as NUM_PERIODO_RIF,${colonna_valore} as IMP_VALORE "
                                           "FROM TABELLA where cod_abi=${cod_abi} and provenienza='${cod_provenienza}'",
            query_ctx
        )
        query_result = "SELECT 202510 as NUM_PERIODO_RIF,valore_p1 as IMP_VALORE FROM TABELLA where cod_abi=3239 and provenienza='PR'"
        self.assertEqual(query_text,query_result)

    def test_return_correct_query_without_all_parameters(self):
        query_ctx = TaskContext(
                    task=TaskSemaforo("uid1", "source_id", "destination_id","gruppo_1",{"k1_1": "key1_1", "k2": "key2_1"},
                                      {"p1_1": "param1_1", "p2_1": "param2_1"}),
                    key={"k1_1": "key1_1", "k2": "key2_1"},
                    query_params={"num_periodo_rif": 202510, "colonna_valore": "valore_p1", "cod_abi": 3239, "cod_provenienza": "PR"}, registro_repo=None
                )
        query_text = QueryResolver.resolve("SELECT ${num_periodo_rif} as NUM_PERIODO_RIF,${colonna_valore} as IMP_VALORE FROM TABELLA",
            query_ctx
        )
        query_result = "SELECT 202510 as NUM_PERIODO_RIF,valore_p1 as IMP_VALORE FROM TABELLA"

        self.assertEqual(query_text, query_result)

    def test_return_correct_query_with_task_context_is_none(self):
        query_text = QueryResolver.resolve("SELECT * FROM TABELLA",None)
        query_result = "SELECT * FROM TABELLA"

        self.assertEqual(query_text, query_result)

    def test_return_correct_query_with_no_query_params(self):
        query_ctx = TaskContext(
                    task=TaskSemaforo("uid1", "source_id", "destination_id","gruppo_1",{"k1_1": "key1_1", "k2": "key2_1"},
                                      {"p1_1": "param1_1", "p2_1": "param2_1"}),
                    key={"k1_1": "key1_1", "k2": "key2_1"},
                    query_params={}, registro_repo=None
                )
        query_text = QueryResolver.resolve("SELECT * FROM TABELLA", query_ctx)
        query_result = "SELECT * FROM TABELLA"

        self.assertEqual(query_text, query_result)