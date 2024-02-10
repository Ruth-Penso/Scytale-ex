from pyspark.sql import functions as F, DataFrame

from transformations.transmormation import Transformation


class FilterSizeTransformation(Transformation):
    def __init__(self, column, condition):
        self.column = column
        self.condition = condition

    def apply(self, dataframe: DataFrame, spark):
        return F.size(F.expr(f"filter({self.column}, {self.condition})")).alias(self.column)
