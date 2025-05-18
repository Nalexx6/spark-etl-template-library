from pyspark.sql import SparkSession, DataFrame
from etl.readers.batch.batch_data_reader import BatchDataReader

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CassandraReader(BatchDataReader):
    def __init__(self, keyspace: str, table: str, options: dict = None, **kwargs):
        self.keyspace = keyspace
        self.table = table
        self.options = options or {}

        super().__init__(**kwargs)

    def read_raw_input(self, spark: SparkSession) -> DataFrame:
        return spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table=self.table, keyspace=self.keyspace, **self.options) \
            .load()
