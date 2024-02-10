from pyspark.sql import DataFrame, SparkSession

from services.service import Service


class DataExtractor:

    def __init__(self, service: Service):
        self.service = service

    def process(self, dataframe: DataFrame, sc: SparkSession) -> None:
        self.service.write(self.service.extract(sc))
