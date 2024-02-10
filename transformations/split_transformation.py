from pyspark.sql import functions as F, DataFrame

from transformations.transmormation import Transformation


class SplitTransformation(Transformation):
    def __init__(self, column, delimiter, index):
        self.column = column
        self.delimiter = delimiter
        self.index = index

    def apply(self, dataframe: DataFrame, spark):
        return F.split(F.col(self.column), self.delimiter)[self.index].alias(self.column)
