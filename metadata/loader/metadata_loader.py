import json
from datetime import datetime

import psycopg2

from common.utils import get_logger
from metadata.models.tab_config import Config
from metadata.models.tab_file import TabFileSource, TabFileDest
from metadata.models.tab_jdbc import TabJDBCSource, TabJDBCDest
from metadata.models.tab_tasks import TaskType, TaskSemaforo
from processor.domain import TaskState, ProcessorType

logger = get_logger(__name__)


class MetadataLoader:

    def __init__(self, meta_db_conn):
        self.conn = psycopg2.connect(**meta_db_conn)

    def fetchall(self, sql, params=None):
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()

    def fetchone(self, sql: str, params=None):
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()

    def execute(self, sql: str, params=None):
        with self.conn.cursor() as cur:
            try:
                cur.execute(sql, params)
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

class CommonMetadata:

    def __init__(self, loader: MetadataLoader):
        self._loader = loader

    def get(self, config_name) -> Config:
        sql = f"SELECT config_name, config_value FROM public.tab_configurations where config_name = ANY(%s)"
        row = self._loader.fetchone(sql,(config_name,))
        return Config(*row[:-1])

    def get_all(self) -> [Config]:
        sql = f"SELECT config_name, config_value FROM public.tab_configurations"
        rows = self._loader.fetchall(sql)
        result = []
        for r in rows:
            result.append(Config(*r[:-1]))
        return result


class OrchestratorMetadata:

    def __init__(self, loader: MetadataLoader):
        self._loader = loader

    def get_all_tasks_in_group(self, groups: [str]) -> [TaskSemaforo]:
        sql = (f'SELECT uid, source_id, destination_id, tipo_caricamento, "key", query_param FROM public.tab_semaforo_ready '
               f'where tipo_caricamento = ANY(%s)')
        rows = self._loader.fetchall(sql, (groups,))
        return [TaskSemaforo(*r) for r in rows]

    def get_task(self, task_id) -> TaskSemaforo:
        sql=f"SELECT * FROM public.tab_tasks_semaforo where uid = %s"
        row = self._loader.fetchone(sql, (task_id,))
        return TaskSemaforo(*row)

    def get_task_configuration(self, task_config_profile: str) -> TaskType:
        sql = "SELECT * FROM public.tab_task_configs WHERE name = ANY (%s) ORDER BY CASE WHEN name = %s THEN 0 WHEN name = 'DEFAULT' THEN 1 END LIMIT 1"
        row = self._loader.fetchone(
            sql,
            ([task_config_profile, "DEFAULT"], task_config_profile)
        )
        if row is None:
            return TaskType.default()
        return TaskType(*row)

class ProcessorMetadata:

    def __init__(self, loader: MetadataLoader):
        self._loader = loader

    def get_task_is_blocking(self, task_id: str) -> bool:
        sql = "SELECT coalesce(is_blocking,True) as is_blocking FROM public.tab_tasks where id = %s'"
        row = self._loader.fetchone(sql,(task_id,))
        return bool(row[0])

    def get_task_processor_type(self, task_id: str) -> str:
        sql ="select processor_type from public.tab_task_configs where tab_task_configs.name= %s"
        row = self._loader.fetchone(sql, (task_id,))
        if row is None:
            return ProcessorType.SPARK.value
        return row[0]

    def get_source_info(self, task_id: str) -> (str,str):
        sql=("SELECT b.source_id, b.source_type FROM public.tab_semaforo_ready a join public.tab_task_sources b "
            "on a.source_id = b.source_id  where a.uid =%s")
        row = self._loader.fetchone(sql, (task_id,))
        return row

    def get_jdbc_source_info(self, source_id: str) -> TabJDBCSource:
        sql = (
            f"SELECT url,username,pwd,driver,tablename,query_text,partitioning_expression,num_partitions "
            f"FROM public.tab_jdbc_sources where source_id = %s")
        row = self._loader.fetchone(sql, (source_id,))
        return TabJDBCSource(*row)

    def get_file_source_info(self, source_id: str) -> TabFileSource:
        sql = "SELECT file_type,path,excel_sheet,csv_separator FROM public.tab_file_sources where source_id = %s"
        row = self._loader.fetchone(sql, (source_id,))
        return TabFileSource(*row)

    def get_destination_info(self, task_id: str) -> (str,str):
        sql = (f"SELECT b.destination_id, b.destination_type FROM public.tab_semaforo_ready a join public.tab_task_destinations b "
                    f"on a.destination_id = b.destination_id where a.uid = %s")
        row = self._loader.fetchone(sql, (task_id,))
        return row

    def get_jdbc_dest_info(self, destination_id: str) -> TabJDBCDest:
        sql = "SELECT url,username,pwd,driver,tablename, columns, overwrite FROM public.tab_jdbc_destinations where destination_id = %s"
        row = self._loader.fetchone(sql, (destination_id,))
        return TabJDBCDest(*row)

    def get_file_dest_info(self, destination_id: str) -> TabFileDest:
        sql = "SELECT format_file,gcs_path,csv_separator,overwrite FROM public.tab_file_destinations where destination_id = %s"
        row = self._loader.fetchone(sql, (destination_id,))
        return TabFileDest(*row)

    def get_task_group(self, task_id) -> str:
        sql = "SELECT cod_gruppo FROM public.tab_tasks_semaforo where uid = %s"
        row = self._loader.fetchone(sql, (task_id,))
        return row[0]

class RegistroMetadata:
    def __init__(self, loader: MetadataLoader):
        self._loader = loader

    def upsert(
            self,
            *,
            chiave: dict,
            last_id: int,
            max_data_va: int = None
    ):
        sql = """
        INSERT INTO public.tab_registro_mensile (chiave, last_id, max_data_va, updated_at)
        VALUES (%(chiave)s,
        %(last_id)s,
        %(max_data_va)s, NOW())
        ON CONFLICT (chiave)
        DO UPDATE SET
            last_id = EXCLUDED.last_id,
            max_data_va = COALESCE(EXCLUDED.max_data_va, tab_registro_mensile.max_data_va),
            updated_at = NOW()
        WHERE tab_registro_mensile.last_id < EXCLUDED.last_id
        """
        self._loader.execute(sql, {
            "chiave": json.dumps(chiave),
            "last_id": last_id,
            "max_data_va": max_data_va
        })

class TaskLogRepository:

    def __init__(self, loader: MetadataLoader):
        self._loader = loader

    def insert_task_log_running(self, task_id: str, run_id: str, description: str = ""):
        self.insert_task_log(
            task_id=task_id,
            run_id=run_id,
            task_state=TaskState.RUNNING,
            task_log_description=description,
        )

    def insert_task_log_successful(self, task_id: str, run_id: str, description: str = "", rows_affected: int = 0):
        self.insert_task_log(
            task_id=task_id,
            run_id=run_id,
            task_state=TaskState.SUCCESSFUL,
            task_log_description=description,
            rows_affected=rows_affected
        )

    def insert_task_log_failed(self, task_id: str, run_id: str, error_message: str, description: str = ""):
        self.insert_task_log(
            task_id=task_id,
            run_id=run_id,
            task_state=TaskState.FAILED,
            task_log_description=description,
            error_message= error_message
        )

    def insert_task_log_warning(self, task_id: str, run_id: str, error_message: str, description: str = ""):
        self.insert_task_log(
            task_id=task_id,
            run_id=run_id,
            task_state=TaskState.WARNING,
            task_log_description=description,
            error_message= error_message
        )

    def get_task_group(self, task_id) -> str:
        sql = "SELECT cod_gruppo FROM public.tab_tasks_semaforo where uid = %s"
        row = self._loader.fetchone(sql, (task_id,))
        return row[0]

    def insert_task_log(
            self,
            task_id: str,
            run_id: str,
            task_state: TaskState,
            task_log_description: str = "",
            error_message: str = "",
            update_ts: datetime = None,
            rows_affected: int = 0
    ):
        if update_ts is None:
            update_ts = datetime.now()

        #group = self.get_task_group(task_id)

        sql = """
        INSERT INTO public.tab_task_logs (
            task_id,
            run_id,
            state_id,
            description,
            error_message,
            update_ts,
            task_group,
            rows_affected
        )
        VALUES (
            %(task_id)s,
            %(run_id)s,
            %(state_id)s,
            %(description)s,
            %(error_message)s,
            %(update_ts)s,
            %(task_group)s,
            %(rows_affected)s
        )
        """

        params = {
            "task_id": task_id,
            "run_id": run_id,
            "state_id": task_state.value,
            "description": task_log_description,
            "error_message": error_message,
            "update_ts": update_ts,
            "task_group": "group",
            "rows_affected": rows_affected,
        }

        self._loader.execute(sql, params)
