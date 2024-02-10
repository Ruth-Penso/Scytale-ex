from pyspark.sql import functions as F, DataFrame

from transformations.transmormation import Transformation


class MergedAtTransformation(Transformation):
    def __init__(self, column):
        self.column = column

    def apply(self, dataframe: DataFrame, spark):
        exploded_df = dataframe.selectExpr(f"explode({self.column}) as exploded_column")
        max_exploded_value = exploded_df.agg(F.max("exploded_column")).collect()[0][0]
        merged_at_value = max_exploded_value if max_exploded_value is not None else '1900-01-01'
        merged_at_expr = F.lit(merged_at_value).cast('timestamp').alias('merged_at')
        return merged_at_expr
