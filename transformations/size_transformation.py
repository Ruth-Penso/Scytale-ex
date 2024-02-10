from pyspark.sql import functions as F, DataFrame

from transformations.transmormation import Transformation


class SizeTransformation(Transformation):
    def __init__(self, column):
        self.column = column

    def apply(self, dataframe: DataFrame, spark):
        return F.size(F.col(self.column)).alias(self.column)
