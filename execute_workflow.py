from typing import List

from pyspark.sql import SparkSession, DataFrame

from steps.processor import Processor


class ExecuteWorkflow:
    def __init__(self, dataframe: DataFrame, spark: SparkSession):
        self.dataframe = dataframe
        self.spark = spark

    def execute(self, workflow: List[Processor]) -> None:
        for step in workflow:
            self.dataframe = step.process(self.dataframe, self.spark)
