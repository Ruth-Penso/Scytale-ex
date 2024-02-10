from pyspark.sql import DataFrame, SparkSession


class Processor:
    def process(self, dataframe: DataFrame, sc: SparkSession) -> DataFrame:
        raise NotImplemented()
