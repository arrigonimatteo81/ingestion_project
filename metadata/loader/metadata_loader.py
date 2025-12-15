import psycopg2
from datetime import datetime

from common.utils import get_logger
from metadata.models.tab_jdbc import TabJDBCSource, TabJDBCDest
from metadata.models.tab_config import Config
from metadata.models.tab_groups import Group
from metadata.models.tab_log import TaskLog
from metadata.models.tab_tasks import Task, TaskType
from processor.domain import TaskState

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

    def get_task_is_blocking(self, task_id: str) -> bool:
        cur = self.conn.cursor()
        cur.execute(
            f"SELECT coalesce(is_blocking,True) as is_blocking FROM public.tab_tasks where id ='{task_id}'")
        row = cur.fetchone()
        return bool(row[0])

    def get_task_processor_type(self, task_id: str) -> str:
        cur = self.conn.cursor()
        cur.execute(f"SELECT processor_type FROM public.tab_task_configs where name ='{task_id}'")
        row = cur.fetchone()
        return row[0]

    def get_source_info(self, task_id: str) -> (str,str):
        cur = self.conn.cursor()
        cur.execute(f"SELECT source_id, source_type FROM public.tab_tasks a join tab_task_sources b on a.source_id = b.source_id "
                    f"where id ='{task_id}'")
        row = cur.fetchone()
        return row[0]

    def get_jdbc_source_info(self, source_id: str) -> TabJDBCSource:
        cur = self.conn.cursor()
        cur.execute(
            f"SELECT url,username,pwd,driver,tablename,query_text,partitioning_expression,num_partitions "
            f"FROM public.tab_jdbc_sources where source_id ='{source_id}'")
        row = cur.fetchone()
        return TabJDBCSource(*row)

    def get_destination_info(self, task_id: str) -> (str,str):
        cur = self.conn.cursor()
        cur.execute(f"SELECT destination_id, destination_type FROM public.tab_tasks a join tab_task_destinations b on a.destination_id = b.destination_id "
                    f"where id ='{task_id}'")
        row = cur.fetchone()
        return row[0]

    def get_jdbc_dest_info(self, destination_id: str) -> TabJDBCDest:
        cur = self.conn.cursor()
        cur.execute(
            f"SELECT url,username,pwd,driver,tablename, overwrite FROM public.tab_jdbc_destinations where destination_id ='{destination_id}'")
        row = cur.fetchone()
        return TabJDBCDest(*row)

    def get_task_group(self, task_id) -> str:
        cur = self.conn.cursor()
        cur.execute(f"SELECT group_name FROM public.tab_task_group where task_id ='{task_id}'")
        row = cur.fetchone()
        return row[0]

    def save_task_log(self, log: TaskLog):
        cur = self.conn.cursor()
        cur.execute(f"INSERT INTO public.tab_task_logs values('{log.task}','{log.run_id}','{log.state.value}','{log.description}',"
                    f"'{log.error_message}','{log.update_ts}','{log.task_group}')")


class LogMetadata(ProcessorMetadata):

    def _insert_task_log(
            self,
            task_id: str,
            run_id: str,
            task_state: TaskState,
            task_log_description: str = "",
            error_message: str = "",
            update_ts: datetime = datetime.now(),
            rows_affected: int = 0
    ):
        if update_ts is None:
            update_ts = datetime.now()
        group: str = self.get_task_group(task_id)
        task_log = TaskLog(
            task=task_id,
            run_id=run_id,
            state=task_state,
            task_group=group,
            description=task_log_description,
            error_message=error_message,
            update_ts=update_ts
        )
        self.save_task_log(task_log)


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
