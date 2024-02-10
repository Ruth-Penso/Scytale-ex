from pyspark.sql import SparkSession

from services.service import Service


class S3Service(Service):
    def __init__(self):
        self.connector = ""

    def extract(self, sc: SparkSession):
        ...

    def write(self, data, file_format=None, path=None):
        ...
