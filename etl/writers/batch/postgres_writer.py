from pyspark.sql import DataFrame
from etl.interfaces import DataWriter

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PostgresWriter(DataWriter):
    def __init__(self, url: str, table: str, mode: str = "append", properties: dict = None):
        self.url = url
        self.table = table
        self.mode = mode
        self.properties = properties

    def write(self, df: DataFrame):
        df.write.jdbc(
            url=self.url,
            table=self.table,
            mode=self.mode,
            properties=self.properties
        )
