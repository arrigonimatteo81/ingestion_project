from common.configuration import DataprocConfiguration
from common.environment import Environment

EST_POLL_SLEEP_TIME_SECONDS = 120
TEST_CLUSTER = "test-cluster"
TEST_CLUSTER_LABELS_JSON = '{"acronimo": "fdir0", "label2": "val2"}'
TEST_CLUSTER_LABELS: dict = {"acronimo": "fdir0", "label2": "val2"}
TEST_PROJECT_NAME = "test-project"
TEST_REGION = "europe-west12"
TEST_CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": "1",
        "machine_type_uri": f"https://www.googleapis.com/compute/v1/projects/prj-isp-fdir0-appl-svil-001/zones/europe-west12-a/machineTypes/n2d-standard-8",
        "disk_config": {
            "boot_disk_type": "pd-balanced",
            "boot_disk_size_gb": "100"
        }
    }
}  # This is a incomplete comfig, only for testing purpose

TEST_POLL_SLEEP_TIME_SECONDS = 120

TEST_CLUSTER_CONFIG_JSON = '{ "master_config": { "num_instances": "1", "machine_type_uri": "https://www.googleapis.com/compute/v1/projects/prj-isp-fdir0-appl-svil-001/zones/europe-west12-a/machineTypes/n2d-standard-8", "disk_config": { "boot_disk_type": "pd-balanced", "boot_disk_size_gb": "100"}}}'  # This is a incomplete comfig, only for testing purpose
TEST_DATAPROC_CONFIGURATION = DataprocConfiguration(
    project=TEST_PROJECT_NAME,
    region=TEST_REGION,
    cluster_name=TEST_CLUSTER,
    poll_sleep_time_seconds=TEST_POLL_SLEEP_TIME_SECONDS,
    environment=Environment.SVIL,
)

TEST_DATAPROC_CONFIGURATION_WITH_MANAGED_CLUSTER_PLACEMENT = DataprocConfiguration(
    project=TEST_PROJECT_NAME,
    region=TEST_REGION,
    cluster_name=TEST_CLUSTER,
    poll_sleep_time_seconds=TEST_POLL_SLEEP_TIME_SECONDS,
    environment=Environment.SVIL
)
TEST_CONFIG_FILE = "gs://mybucket//config.yaml"

TEST_CLUSTER_NAME = "my-cluster"
TEST_ENVIRONMENT = Environment.SVIL
