from pyspark.sql.types import StructType

from configuration.config import DEST_URL
from connectors.spark_connector import SparkConnector
from execute_workflow import ExecuteWorkflow
from services.github_service import GithubService
from services.local_service import LocalService
from steps import create_workflow


def run() -> None:
    sc = SparkConnector().create_connector()
    workflow = create_workflow(GithubService(), LocalService(), LocalService(), DEST_URL)
    exec_workflow = ExecuteWorkflow(sc.createDataFrame([], StructType([])), sc)
    exec_workflow.execute(workflow=workflow)


if __name__ == '__main__':
    run()
