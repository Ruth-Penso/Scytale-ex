from pyspark.sql import DataFrame, SparkSession

from services.service import Service
from steps.processor import Processor


class DataLoader(Processor):

    def __init__(self, service: Service):
        self.service = service

    def process(self, dataframe: DataFrame, sc: SparkSession) -> DataFrame:
        dataframe = self.service.extract(sc)
        dataframe.cache()
        return dataframe
