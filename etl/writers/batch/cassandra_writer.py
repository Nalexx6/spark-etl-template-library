from pyspark.sql import DataFrame
from etl.interfaces import DataWriter

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CassandraWriter(DataWriter):
    def __init__(self, keyspace: str, table: str, mode: str = "append", options: dict = None, **kwargs):
        self.keyspace = keyspace
        self.table = table
        self.mode = mode
        self.options = options or {}

    def write(self, df: DataFrame):
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table=self.table, keyspace=self.keyspace, **self.options) \
            .mode(self.mode) \
            .save()
