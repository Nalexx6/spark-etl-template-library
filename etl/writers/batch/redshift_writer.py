from pyspark.sql import DataFrame
from etl.interfaces import DataWriter

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RedshiftWriter(DataWriter):
    def __init__(self, url: str, table: str, mode: str = "append", options: dict = None):
        self.url = url
        self.table = table
        self.options = options
        self.mode = mode

    def write(self, df: DataFrame):
        df.write \
            .format("jdbc") \
            .option("url", self.url) \
            .option("dbtable", self.table) \
            .options(**self.options) \
            .mode(self.mode) \
            .save()
