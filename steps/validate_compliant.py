from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from configuration.consts import IS_COMPLIANT
from steps.processor import Processor


class ValidateCompliant(Processor):
    def process(self, dataframe: DataFrame, sc: SparkSession) -> DataFrame:
        return dataframe.withColumn(IS_COMPLIANT,
                                    F.when((F.col("num_prs") == F.col("num_prs_merged")) &
                                           (F.col("repository_owner").contains("Scytale")),
                                           True).otherwise(False))
