from metadata.loader.metadata_loader import OrchestratorMetadata, MetadataLoader
from metadata.models.tab_groups import Group


class TestMetadataLoader(MetadataLoader):
    def __init__(self, con):
        super().__init__(con)

class TestOrchestratorMetadata(OrchestratorMetadata):
    def __init__(self, loader: MetadataLoader) -> None:
        pass #super().__init__(loader)

    def get_all_tasks_in_group(self, groups: [str]) -> [Group]:
        return[Group("DM", "GRP1"), Group("KPI1", "GRP1"),
               Group("KPI2", "GRP1"), Group("REPORT1", "GRP1"),
               Group("REPORT2", "GRP1")]