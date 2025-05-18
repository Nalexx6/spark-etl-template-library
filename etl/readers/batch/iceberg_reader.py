from pyspark.sql import DataFrame, SparkSession
from etl.readers.batch.batch_data_reader import BatchDataReader

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class IcebergReader(BatchDataReader):
    def __init__(self, table: str, options: dict = None, **kwargs):
        self.table = table
        self.options = options or {}

        super().__init__(**kwargs)

    def read_raw_input(self, spark: SparkSession) -> DataFrame:
        return spark.read \
            .format("iceberg") \
            .options(**self.options) \
            .load(self.table)
