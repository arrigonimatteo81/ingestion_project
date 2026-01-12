import unittest

from helpers.query_resolver import TaskContext, QueryResolver
from metadata.loader.metadata_loader import RegistroRepository, MetadataLoader
from metadata.models.tab_tasks import TaskSemaforo
import json
from unittest.mock import MagicMock


class TestQueryRenderer(unittest.TestCase):

    def test_return_correct_query_with_all_parameters(self):
        query_ctx = TaskContext(
                    task=TaskSemaforo("uid1", "source_id", "destination_id","gruppo_1",{"k1_1": "key1_1", "k2": "key2_1"},
                                      {"p1_1": "param1_1", "p2_1": "param2_1"}),
                    key={"k1_1": "key1_1", "k2": "key2_1"},
                    query_params={"num_periodo_rif": 202510, "colonna_valore": "valore_p1", "cod_abi": 3239, "cod_provenienza": "PR"},
                    run_id="run_id"
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
                    query_params={"id": 8, "num_periodo_rif": 202510, "colonna_valore": "valore_p1", "cod_abi": 3239, "cod_provenienza": "PR"},
                    run_id="run_id"                )
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
                    query_params={}, run_id="run_id"
                )
        query_text = QueryResolver.resolve("SELECT * FROM TABELLA", query_ctx)
        query_result = "SELECT * FROM TABELLA"

        self.assertEqual(query_text, query_result)

class TestQueryUpsertRegistro(unittest.TestCase):

    def test_upsert_with_id_and_max_data_va(self):
        loader = MagicMock()
        repo = RegistroRepository(loader)

        chiave = {
            "cod_abi": 123,
            "cod_tabella": "CLIENTI",
            "cod_provenienza": "SRC"
        }

        repo.upsert(
            chiave=chiave,
            last_id=10,
            max_data_va=20241231
        )

        loader.execute.assert_called_once()

        sql, params = loader.execute.call_args[0]

        assert "INSERT INTO public.tab_registro_mensile" in sql
        assert "ON CONFLICT (chiave)" in sql
        assert "EXCLUDED.last_id" in sql

        assert params == {
            "chiave": '{"cod_abi": 123, "cod_tabella": "CLIENTI", "cod_provenienza": "SRC"}',
            "last_id": 10,
            "max_data_va": 20241231,
        }

    def test_upsert_without_max_data_va(self):
        loader = MagicMock()
        repo = RegistroRepository(loader)

        chiave = {"cod_tabella": "DOMINI_X"}

        repo.upsert(
            chiave=chiave,
            last_id=5,
            max_data_va=None
        )

        loader.execute.assert_called_once()
        sql, params = loader.execute.call_args[0]

        assert params == {
            "chiave": '{"cod_tabella": "DOMINI_X"}',
            "last_id":5,
            "max_data_va":None
        }
