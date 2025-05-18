from pyspark.sql import DataFrame
from etl.interfaces import DataWriter

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class IcebergWriter(DataWriter):
    def __init__(self, table: str, mode: str = "append", options: dict = None):
        self.table = table
        self.options = options or {}
        self.mode = mode

    def write(self, df: DataFrame):
        df.write \
            .format("iceberg") \
            .options(**self.options) \
            .mode(self.mode) \
            .saveAsTable(self.table)
