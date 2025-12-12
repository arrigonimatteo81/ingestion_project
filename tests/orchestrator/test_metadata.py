from metadata.loader.metadata_loader import OrchestratorMetadata
from metadata.models.tab_groups import TabGroups


class TestOrchestratorMetadata(OrchestratorMetadata):
    def __init__(self) -> None:
        pass

    def get_all_tasks_in_group(self, groups: [str]) -> [TabGroups]:
        return[TabGroups("DM","GRP1"),TabGroups("KPI1","GRP1"),
               TabGroups("KPI2","GRP1"),TabGroups("REPORT1","GRP1"),
               TabGroups("REPORT2","GRP1")]