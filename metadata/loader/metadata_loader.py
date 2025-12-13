import psycopg2

from common.utils import get_logger
from metadata.models.tab_config import Config
from metadata.models.tab_groups import Group
from metadata.models.tab_tasks import Task, TaskType

logger = get_logger(__name__)


class MetadataLoader:

    def __init__(self, meta_db_conn):
        self.conn = psycopg2.connect(**meta_db_conn)


class CommonMetadata(MetadataLoader):

    def get(self, config_name) -> Config:
        cur = self.conn.cursor()
        cur.execute(
            f"SELECT config_name, config_value FROM public.tab_configurations where config_name='{config_name}'")
        row = cur.fetchone()
        return Config(*row[:-1])

    def get_all(self) -> [Config]:
        cur = self.conn.cursor()
        cur.execute(f"SELECT config_name, config_value FROM public.tab_configurations")
        rows = cur.fetchall()
        result = []
        for r in rows:
            result.append(Config(*r[:-1]))
        return result


class OrchestratorMetadata(MetadataLoader):

    def get_all_tasks_in_group(self, groups: [str]) -> [Group]:
        cur = self.conn.cursor()
        str_group = "',".join(groups)
        logger.debug(str_group)
        cur.execute(f"SELECT * FROM public.tab_task_group where group_name in ('{str_group}')")
        rows = cur.fetchall()
        result = []
        for r in rows:
            result.append(Group(*r))
        return result

    def get_task(self, task_id) -> Task:
        cur = self.conn.cursor()
        cur.execute(f"SELECT * FROM public.tab_tasks where id ='{task_id}'")
        row = cur.fetchone()
        return Task(*row)

    def get_task_configuration(self, task_config_profile: str) -> TaskType:
        cur = self.conn.cursor()
        cur.execute(f"SELECT * FROM public.tab_task_configs where name ='{task_config_profile}'")
        row = cur.fetchone()
        return TaskType(*row)

class ProcessorMetadata(MetadataLoader):
    def get_task_processor_type(self, task_id: str) -> str:
        cur = self.conn.cursor()
        cur.execute(f"SELECT processor_type FROM public.tab_task_configs where name ='{task_id}'")
        row = cur.fetchone()
        return row[0]



    """def load_connections(self):
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM tab_conn")
        rows = cur.fetchall()
        result = {}
        for r in rows:
            result[r[0]] = TabConn(*r[:-1], extra_params=json.loads(r[-1]) if r[-1] else {}).__dict__
        return result

    def load_destinations(self):
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM tab_dest")
        rows = cur.fetchall()
        result = {}
        for r in rows:
            result[r[0]] = TabDest(*r[:-1], extra_params=json.loads(r[-1]) if r[-1] else {}).__dict__
        return result

    def load_sources(self):
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM tab_src")
        rows = cur.fetchall()
        result = []
        for r in rows:
            result.append(TabSrc(*r).__dict__)
        return result"""
