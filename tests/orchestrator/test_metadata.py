from metadata.loader.metadata_loader import OrchestratorMetadata
from metadata.models.tab_groups import Group


class TestOrchestratorMetadata(OrchestratorMetadata):
    def __init__(self) -> None:
        pass

    def get_all_tasks_in_group(self, groups: [str]) -> [Group]:
        return[Group("DM", "GRP1"), Group("KPI1", "GRP1"),
               Group("KPI2", "GRP1"), Group("REPORT1", "GRP1"),
               Group("REPORT2", "GRP1")]