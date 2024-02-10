from pyspark.sql import functions as F, DataFrame

from transformations.transmormation import Transformation


class MaxTransformation(Transformation):
    def __init__(self, column):
        self.column = column

    def apply(self, dataframe: DataFrame, spark):
        max_exploded_column = F.max(F.explode(F.col(self.column))).alias(self.column)
        return dataframe.select(max_exploded_column)
