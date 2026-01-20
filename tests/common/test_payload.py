import unittest

from common.task_semaforo_payload import TaskSemaforoPayload
from metadata.models.tab_tasks import TaskSemaforo

class TestTaskSemaforoPayload(unittest.TestCase):
    def test_task_semaforo_payload_to_json(self):
        task=TaskSemaforo('uid1','source_1','destination_1','gruppo_1',{'k1_1':'key1_1','k2':'key2_1'}, {'p1_1':'param1_1', 'p2_1': 'param2_1'}, is_heavy=True)
        payload: TaskSemaforoPayload = TaskSemaforoPayload(task.uid, task.source_id, task.destination_id,
                                                           task.tipo_caricamento,
                                                           task.key, task.query_params)
        self.assertEqual(payload.to_json(),'{"uid": "uid1", "source_id": "source_1", "destination_id": "destination_1", "tipo_caricamento": "gruppo_1", "key": {"k1_1": "key1_1", "k2": "key2_1"}, "query_params": {"p1_1": "param1_1", "p2_1": "param2_1"}}')

    def test_task_semaforo_payload_from_json(self):
        payload: TaskSemaforoPayload = TaskSemaforoPayload.from_json('{"uid": "uid1", "source_id": "source_id", "destination_id": "destination_id", "tipo_caricamento": "gruppo_1", "key": {"k1_1": "key1_1", "k2": "key2_1"}, "query_params": {"p1_1": "param1_1", "p2_1": "param2_1"}}')
        expected: TaskSemaforoPayload = TaskSemaforoPayload("uid1", "source_id", "destination_id",
                                                           "gruppo_1",
                                                           {"k1_1": "key1_1", "k2": "key2_1"}, {"p1_1": "param1_1", "p2_1": "param2_1"})
        self.assertEqual(payload,expected)

    def test_task_semaforo_payload_to_domain(self):
        task_payload: TaskSemaforoPayload  = TaskSemaforoPayload.from_json('{"uid": "uid1", "source_id": "source_id", "destination_id": "destination_id", "tipo_caricamento": "gruppo_1", "key": {"k1_1": "key1_1", "k2": "key2_1"}, "query_params": {"p1_1": "param1_1", "p2_1": "param2_1"}}')
        result: TaskSemaforo = task_payload.to_domain()
        expected: TaskSemaforo = TaskSemaforo("uid1", "source_id", "destination_id","gruppo_1",{"k1_1": "key1_1", "k2": "key2_1"}, {"p1_1": "param1_1", "p2_1": "param2_1"})
        self.assertEqual(result,expected)

