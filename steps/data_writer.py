from pyspark.sql import DataFrame, SparkSession

from services.service import Service
from steps.processor import Processor


class DataWriter(Processor):
    def __init__(self, service: Service, path, file_format):
        self.service = service
        self.path = path
        self.file_format = file_format

    def process(self, dataframe: DataFrame, sc: SparkSession):
        self.service.write(dataframe, self.file_format, self.path)
