from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from configuration.config import TRANSFORMATIONS
from steps.processor import Processor


class DataProcessor(Processor):
    def process(self, dataframe: DataFrame, spark: SparkSession) -> DataFrame:
        select_exprs = []
        for target_column, transformation in TRANSFORMATIONS.items():
            if isinstance(transformation, str):
                expr = F.expr(transformation).alias(target_column)
            else:
                expr = transformation.apply(dataframe, spark).alias(target_column)
            select_exprs.append(expr)
        dataframe = dataframe.select(*select_exprs)
        return dataframe
