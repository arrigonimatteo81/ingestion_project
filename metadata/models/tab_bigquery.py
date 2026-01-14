from dataclasses import dataclass

@dataclass
class TabBigQuerySource:
    project: str
    dataset: str
    tablename: str
    query_text: str = None

    def __repr__(self):
        return f"TabBigQuerySource(project={self.project},dataset={self.dataset},tablename={self.tablename},query_text={self.query_text})"

@dataclass
class TabBigQueryDest:
    project: str
    dataset: str
    tablename: str
    overwrite: bool = False

    def __repr__(self):
        return (f"TabBigQueryDest(project={self.project},dataset={self.dataset},tablename={self.tablename},"
                f"overwrite={self.overwrite})")

class BigQuery:

    format = "bigquery"

    def __init__(self, project: str, dataset: str):
        self._project = project
        self._dataset = dataset

    @property
    def project(self):
        return self._project

    @property
    def dataset(self):
        return self._dataset

class BigQueryTable(BigQuery):
    def __init__(self,project: str, dataset: str, table: str):

        # Initialize the base JDBC class with the provided credentials and connection details.
        BigQuery.__init__(self, project,dataset)
        # Set the specific database table for this instance.
        self.table = table

class BigQueryQuery(BigQuery):
    def __init__(self, project: str, dataset: str, query: str):
        # Initialize the base JDBC class with the provided credentials and connection details.
        BigQuery.__init__(self, project, dataset)
        self.query = query