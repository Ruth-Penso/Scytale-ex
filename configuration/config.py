from configuration.consts import REPO_INFO, PULL_REQUESTS
from transformations.filter_size_transformation import FilterSizeTransformation
from transformations.merged_at_transformation import MergedAtTransformation
from transformations.size_transformation import SizeTransformation
from transformations.split_transformation import SplitTransformation

BASE_URL = "https://api.github.com"
EXTRACT_URL = "*.json"
DEST_URL = "file:///output"

SPARK_CONFIG = {
    "spark.some.config.option": "some-value",
    "spark.driver.memory": "4g",
    "spark.executor.memory": "2g",
    "spark.executor.cores": "2",
    "spark.executor.instances": "4",
    "spark.hadoop.fs.defaultFS": "file:///",
    "spark.sql.sources.commitProtocolClass": "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol"
}

TRANSFORMATIONS = {
    "Organization Name": f"{REPO_INFO}.name",
    "repository_id": f"{REPO_INFO}.id",
    "repository_name": f"{REPO_INFO}.name",
    "repository_owner": f"{REPO_INFO}.owner.login",
    "owner_name": SplitTransformation(f"{REPO_INFO}.full_name", "/", 0),
    "num_prs": SizeTransformation(PULL_REQUESTS),
    "num_prs_merged": FilterSizeTransformation(PULL_REQUESTS, "pull_request -> pull_request.merged == true"),
    "merged_at": MergedAtTransformation(f"{PULL_REQUESTS}.merged_at")
}
