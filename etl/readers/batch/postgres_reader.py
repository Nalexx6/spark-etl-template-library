from pyspark.sql import SparkSession, DataFrame
from etl.readers.batch.batch_data_reader import BatchDataReader

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PostgresReader(BatchDataReader):
    def __init__(self, url: str, table: str, properties: dict, **kwargs):
        self.url = url
        self.table = table
        self.properties = properties

        super().__init__(**kwargs)

    def read_raw_input(self, spark: SparkSession) -> DataFrame:
        return spark.read.jdbc(
            url=self.url,
            table=self.table,
            properties=self.properties
        )
