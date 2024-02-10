from pyspark.sql import SparkSession

from configuration.config import SPARK_CONFIG
from connectors.connector import Connector


class SparkConnector(Connector):
    def create_connector(self) -> SparkSession:
        spark_builder = SparkSession.builder \
            .appName("Scytale-Ex") \
            .master("local[*]")
        for key, value in SPARK_CONFIG.items():
            spark_builder.config(key, value)
        return spark_builder.getOrCreate()
