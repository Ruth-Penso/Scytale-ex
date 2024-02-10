from pyspark.sql import SparkSession


class Service:

    def extract(self, sc: SparkSession):
        raise NotImplemented()

    def write(self, data, file_format=None, path=None):
        raise NotImplemented()
