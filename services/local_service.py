from glob import glob

from pyspark.sql import SparkSession, DataFrame

from configuration.config import EXTRACT_URL
from services.service import Service


class LocalService(Service):
    def __init__(self):
        self.connector = glob(EXTRACT_URL)

    def extract(self, sc: SparkSession):
        return sc.read.json(self.connector)

    def write(self, data: DataFrame, file_format=None, path=None):
        data.write.mode("overwrite").parquet(path)
